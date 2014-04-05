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

package com.hazelcast.examples;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.util.HealthMonitorLevel;

/**
 * Starts a Hazelcast server
 */
public final class StartServer {


    private StartServer() {
    }

    /**
     * Creates a server instance of Hazelcast
     *
     * @param args none
     */
    public static void main(String[] args) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");

        Config config = new XmlConfigBuilder().build();
        config.setProperty(GroupProperties.PROP_HEALTH_MONITORING_LEVEL, HealthMonitorLevel.NOISY.toString());

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear().addMember("10.16.33.180")
                .setConnectionTimeoutSeconds(10);

        config.getMapConfig("default").setAsyncBackupCount(1).setBackupCount(0);

        Hazelcast.newHazelcastInstance(config);

        Thread t = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats");
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(10000);
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
}
