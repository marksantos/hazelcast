/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.examples;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mdogan 27/12/13
 */
public class GCUtil {

    static final Set<String> YOUNG_GC = new HashSet<String>(3);
    static final Set<String> OLD_GC = new HashSet<String>(3);

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }


    public static String getGCStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("GC STATS :: ").append('\n');
        sb.append("=================================================================================").append('\n');

        long minorCount = 0;
        long minorTime = 0;

        long majorCount = 0;
        long majorTime = 0;

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                if (YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (OLD_GC.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else {
                    sb.append("Unknown GC: " + gc.getName() + "-> " + Arrays.toString(gc.getMemoryPoolNames()));
                }
            }
        }

        long heapUsage = MemoryUnit.BYTES.toMegaBytes(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        sb.append("MinorGC -> Count: " + minorCount + ", Time (ms): " + minorTime
                + ", MajorGC -> Count: " + majorCount + ", Time (ms): " + majorTime
                + ", Heap Usage (MB): " + heapUsage);

        return sb.toString();
    }

}
