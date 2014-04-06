/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.examples;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.HealthMonitorLevel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleTopicTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats stats = new Stats();

    private final int threadCount;
    private final int sleep;
    private final int valueSize;

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
    }

    private SimpleTopicTest(final int threadCount, final int sleep, final int valueSize) {
        this.threadCount = threadCount;
        this.sleep = sleep;
        this.valueSize = valueSize;
        Config config = new XmlConfigBuilder().build();
        config.setProperty(GroupProperties.PROP_HEALTH_MONITORING_LEVEL, HealthMonitorLevel.NOISY.toString());
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true)
                .clear().addMember("10.16.40.40")
                .setConnectionTimeoutSeconds(10);
        instance = Hazelcast.newHazelcastInstance(config);
        logger = instance.getLoggingService().getLogger("SimpleMapTest");
    }

    public static void main(String[] input) throws InterruptedException {
        int threadCount = 1;
        int valueSize = 1000;
        int sleep = 0;
        boolean load = false;
        if (input != null && input.length > 0) {
            for (String arg : input) {
                arg = arg.trim();
                if (arg.startsWith("t")) {
                    threadCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    valueSize = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("s")) {
                    sleep = Integer.parseInt(arg.substring(1));
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130");
            System.out.println("means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println();
        }
        SimpleTopicTest test = new SimpleTopicTest(threadCount, sleep, valueSize);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
        run(es);
    }

    private Object createValue() {
        return new byte[valueSize];
    }

    private void run(ExecutorService es) {
        final ITopic<Object> topic = instance.getTopic(NAMESPACE);
        topic.addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {
                stats.receives.incrementAndGet();
            }
        });
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                public void run() {
                    while (true) {
                        try {
                            topic.publish(createValue());
                            stats.publishes.incrementAndGet();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Thread Count: " + threadCount);
        logger.info("Sleep: " + sleep);
        logger.info("Value Size: " + valueSize);
    }

    private void startPrintStats() {
        Thread t = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        stats.printAndReset();
                        System.out.println();
                        System.out.println(GCUtil.getGCStats());
                        System.out.println();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        };
        t.start();
    }

    private class Stats {
        public AtomicLong publishes = new AtomicLong();
        public AtomicLong receives = new AtomicLong();

        public void printAndReset() {
            long receivesNow = receives.getAndSet(0);
            long publishesNow = publishes.getAndSet(0);
            logger.info("receives ps:" + receivesNow / STATS_SECONDS
                    + ", publishes ps:" + publishesNow / STATS_SECONDS);
        }

        public String toString() {
            return "receives:" + receives.get() + ", publishes: " + publishes.get();
        }
    }
}
