/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.impl.curator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class CuratorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderSelector.class);
    protected final String name;
    protected final LeaderSelector leaderSelector;

    public CuratorLeaderSelector(CuratorFramework client, String path, String name) {
        this.name = name;
        this.leaderSelector = new LeaderSelector(client, path, this);
        this.leaderSelector.setId(name);
        this.leaderSelector.autoRequeue();
    }

    public Participant getLeader() {
        try {
            return leaderSelector.getLeader();
        } catch (Exception e) {
            logger.error("Can not get leader.", e);
        }
        return new Participant("", false);
    }

    public List<Participant> getParticipants() {
        List<Participant> r = new ArrayList<>();
        try {
            r.addAll(leaderSelector.getParticipants());
        } catch (Exception e) {
            logger.error("Can not get participants.", e);
        }
        return r;
    }

    public void start() {
        leaderSelector.start();
    }

    public boolean hasLeadership() {
        return leaderSelector.hasLeadership();
    }

    @Override
    public void close() throws IOException {
        try {
            leaderSelector.close();
        } catch (IllegalStateException e) {
            if (e.getMessage().equals("Already closed or has not been started")) {
                logger.warn("LeaderSelector already closed or has not been started");
            } else {
                throw e;
            }
        }
        logger.info(name + " is stopped");
    }

}