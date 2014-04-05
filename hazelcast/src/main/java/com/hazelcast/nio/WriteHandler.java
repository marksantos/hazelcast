/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.util.StringUtil.stringToBytes;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private static final ScheduledExecutorService ex = Executors.newScheduledThreadPool(2, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "WRITE-HANDLER");
            t.setDaemon(true);
            return t;
        }
    });


    private final Queue<SocketWritable> writeQueue = new SocketQueue();

    private final Queue<SocketWritable> eventWriteQueue = new SocketQueue();

    private final Queue<SocketWritable> urgencyWriteQueue = new ConcurrentLinkedQueue<SocketWritable>();

    private final AtomicInteger qCounter = new AtomicInteger();
    private final AtomicInteger eCounter = new AtomicInteger();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private final IOSelector ioSelector;

    private boolean ready = false;

    private SocketWritable lastWritable;

    private SocketWriter socketWriter;

    private volatile long lastHandle = 0;

    private int nonEventPollCount;

    WriteHandler(final TcpIpConnection connection, final IOSelector ioSelector) {
        super(connection);
        this.ioSelector = ioSelector;
        buffer = ByteBuffer.allocate(connectionManager.socketSendBufferSize);

        ex.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                if (connection.live()) {
                    int qsize = writeQueue.size();
                    int usize = urgencyWriteQueue.size();
                    int esize = eventWriteQueue.size();

                    if (qsize + esize + usize > 0) {
                        System.err.println
                                (connection.getEndPoint()
                                                + " -> Q: " + qsize
                                                + ", U: " + usize
                                                + ", E: " + esize
                                                + ", QCounter: " + qCounter.getAndSet(0)
                                                + ", ECounter: " + eCounter.getAndSet(0)
                                                + ", B: " + buffer.remaining()
                                                + ", last: " + new Date(lastHandle)
                                );

                        SerializationService ss = connection.getConnectionManager().ioService
                                .getSerializationService();
                        for (SocketWritable sw : urgencyWriteQueue) {
                            if (sw instanceof Packet) {
                                System.err.println("\t" + ss.toObject(((Packet) sw).getData()));
                            }
                        }

                        ioSelector.addTask(WriteHandler.this);
                        ioSelector.wakeup();
                    }

                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    // accessed from ReadHandler and SocketConnector
    void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioSelector.addTask(new Runnable() {
            public void run() {
                createWriter(protocol);
                latch.countDown();
            }
        });
        ioSelector.wakeup();
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    private void createWriter(String protocol) {
        if (socketWriter == null) {
            if (Protocols.CLUSTER.equals(protocol)) {
                socketWriter = new SocketPacketWriter(connection);
                buffer.put(stringToBytes(Protocols.CLUSTER));
                registerWrite();
            } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                socketWriter = new SocketClientDataWriter(connection);
            } else {
                socketWriter = new SocketTextWriter(connection);
            }
        }
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
    }

    public boolean enqueueSocketWritable(SocketWritable socketWritable) {
        if (socketWritable == null) {
            throw new NullPointerException("SocketWritable expected!");
        }

        boolean urgent = socketWritable.isUrgent();
        boolean event = socketWritable.isEvent();

        if (urgent) {
            urgencyWriteQueue.offer(socketWritable);
        } else if (event) {
            eCounter.incrementAndGet();
            int size = eventWriteQueue.size();
            if (size > 10000) {
                return false;
            }
            eventWriteQueue.offer(socketWritable);
        } else {
            qCounter.incrementAndGet();
            int size = writeQueue.size();
            if (size > 10000) {
                return false;
            }
            writeQueue.offer(socketWritable);
        }

        if (informSelector.compareAndSet(true, false)) {
            // we don't have to call wake up if this WriteHandler is
            // already in the task queue.
            // we can have a counter to check this later on.
            // for now, wake up regardless.     `                                               `
            ioSelector.addTask(this);
            ioSelector.wakeup();
        }
        return true;
    }

    private SocketWritable poll() {
        SocketWritable writable = urgencyWriteQueue.poll();
        if (writable == null) {
            if (nonEventPollCount < 1) {
                writable = writeQueue.poll();
                nonEventPollCount++;
            } else {
                writable = eventWriteQueue.poll();
                nonEventPollCount = 0;
            }
        }
        return writable;
    }

    @SuppressWarnings("unchecked")
    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.live()) {
            return;
        }
        if (socketWriter == null) {
            logger.log(Level.WARNING, "SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            createWriter(Protocols.CLUSTER);
        }
        if (lastWritable == null && (lastWritable = poll()) == null && buffer.position() == 0) {
            ready = true;
            return;
        }
        try {
            while (buffer.hasRemaining() && lastWritable != null) {
                boolean complete = socketWriter.write(lastWritable, buffer);
                if (complete) {
                    lastWritable = poll();
                } else {
                    break;
                }
            }
            if (buffer.position() > 0) {
                buffer.flip();
                try {
                    socketChannel.write(buffer);
                } catch (Exception e) {
                    lastWritable = null;
                    handleSocketException(e);
                    return;
                }
                if (buffer.hasRemaining()) {
                    buffer.compact();
                } else {
                    buffer.clear();
                }
            }
        } catch (Throwable t) {
            logger.severe("Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
            connection.getSystemLogService().logConnection("Fatal Error at WriteHandler for endPoint " +
                    "[" + connection.getEndPoint() + "]: " + t.getMessage());
        } finally {
            ready = false;
            registerWrite();
            lastHandle = Clock.currentTimeMillis();
        }
    }

    public void run() {
        informSelector.set(true);
        if (ready) {
            handle();
        } else {
            registerWrite();
        }
        ready = false;
    }

    private void registerWrite() {
        registerOp(ioSelector.getSelector(), SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        while (poll() != null) ;
    }

    long getLastHandle() {
        return lastHandle;
    }

    private static class SocketQueue extends ConcurrentLinkedQueue<SocketWritable> {

        private final AtomicInteger size = new AtomicInteger();

        @Override
        public boolean offer(SocketWritable socketWritable) {
            if (socketWritable == null) {
                throw new NullPointerException();
            }
            boolean offered = super.offer(socketWritable);
            if (offered) {
                size.incrementAndGet();
            }
            return offered;
        }

        @Override
        public SocketWritable poll() {
            SocketWritable sw = super.poll();
            if (sw != null) {
                size.decrementAndGet();
            }
            return sw;
        }

        @Override
        public int size() {
            return size.get();
        }
    }
}
