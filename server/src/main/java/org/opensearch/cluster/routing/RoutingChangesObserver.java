/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.PublicApi;

import java.util.List;

/**
 * Records changes made to {@link RoutingNodes} during an allocation round.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface RoutingChangesObserver {
    /**
     * Called when unassigned shard is initialized. Does not include initializing relocation target shards.
     */
    void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard);

    /**
     * Called when an initializing shard is started.
     */
    void shardStarted(ShardRouting initializingShard, ShardRouting startedShard);

    /**
     * Called when a child replica is started.
     */
    void childReplicaStarted(ShardRouting initializingShard, ShardRouting parentShard, ShardRouting childShard);

    /**
     * Called when a child shard fails.
     */
    void childShardFailed(ShardRouting parentShard, ShardRouting childShard);

    /**
     * Called when relocation of a started shard is initiated.
     */
    void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard);

    /**
     * Called when split of a started shard is initiated.
     */
    void splitStarted(ShardRouting startedShard, List<ShardRouting> childSplitShards);

    /**
     * Called when an unassigned shard's unassigned information was updated
     */
    void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo);

    /**
     * Called when a shard is failed or cancelled.
     */
    void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo);

    /**
     * Called on relocation source when relocation completes after relocation target is started.
     */
    void relocationCompleted(ShardRouting removedRelocationSource);

    /**
     * Called on replica relocation target when replica relocation source fails. Promotes the replica relocation target to ordinary
     * initializing shard.
     */
    void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource);

    /**
     * Called when split completes after child shards are started.
     */
    void splitCompleted(ShardRouting removedSplitSource, IndexMetadata indexMetadata);

    /**
     * Called when in-place split of child shards has failed.
     */
    void splitFailed(ShardRouting splitSource, IndexMetadata indexMetadata);

    /**
     * Called to determine if shard split has failed in current cluster update.
     */
    boolean isSplitOfShardFailed(ShardRouting parentShard);

    /**
     * Called when started replica is promoted to primary.
     */
    void replicaPromoted(ShardRouting replicaShard);

    /**
     * Called when an initializing replica is reinitialized. This happens when a primary relocation completes, which
     * reinitializes all currently initializing replicas as their recovery source node changes
     */
    void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica);

    /**
     * Abstract implementation of {@link RoutingChangesObserver} that does not take any action. Useful for subclasses that only override
     * certain methods.
     */
    class AbstractRoutingChangesObserver implements RoutingChangesObserver {

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {

        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {

        }

        @Override
        public void childReplicaStarted(ShardRouting initializingShard, ShardRouting parentShard, ShardRouting childReplica) {

        }

        @Override
        public void childShardFailed(ShardRouting parentShard, ShardRouting childShard) {

        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {

        }

        @Override
        public void splitStarted(ShardRouting startedShard, List<ShardRouting> childSplitShards) {

        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {

        }

        @Override
        public void shardFailed(ShardRouting activeShard, UnassignedInfo unassignedInfo) {

        }

        @Override
        public boolean isSplitOfShardFailed(ShardRouting parentShard) {
            return false;
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {

        }

        @Override
        public void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {

        }

        @Override
        public void splitCompleted(ShardRouting removedSplitSource, IndexMetadata indexMetadata) {

        }

        @Override
        public void splitFailed(ShardRouting splitSource, IndexMetadata indexMetadata) {

        }

        @Override
        public void replicaPromoted(ShardRouting replicaShard) {

        }

        @Override
        public void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica) {

        }
    }

    /**
     * Observer of routing changes.
     *
     * @opensearch.internal
     */
    class DelegatingRoutingChangesObserver implements RoutingChangesObserver {

        private final RoutingChangesObserver[] routingChangesObservers;

        public DelegatingRoutingChangesObserver(RoutingChangesObserver... routingChangesObservers) {
            this.routingChangesObservers = routingChangesObservers;
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardInitialized(unassignedShard, initializedShard);
            }
        }

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardStarted(initializingShard, startedShard);
            }
        }

        @Override
        public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationStarted(startedShard, targetRelocatingShard);
            }
        }

        @Override
        public void splitStarted(ShardRouting startedShard, List<ShardRouting> childSplitShards) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.splitStarted(startedShard, childSplitShards);
            }
        }

        @Override
        public boolean isSplitOfShardFailed(ShardRouting parentShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                if (routingChangesObserver.isSplitOfShardFailed(parentShard)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.unassignedInfoUpdated(unassignedShard, newUnassignedInfo);
            }
        }

        @Override
        public void shardFailed(ShardRouting activeShard, UnassignedInfo unassignedInfo) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.shardFailed(activeShard, unassignedInfo);
            }
        }

        @Override
        public void relocationCompleted(ShardRouting removedRelocationSource) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationCompleted(removedRelocationSource);
            }
        }

        @Override
        public void relocationSourceRemoved(ShardRouting removedReplicaRelocationSource) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.relocationSourceRemoved(removedReplicaRelocationSource);
            }
        }

        @Override
        public void childReplicaStarted(ShardRouting initializingShard, ShardRouting parentShard, ShardRouting childReplica) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.childReplicaStarted(initializingShard, parentShard, childReplica);
            }
        }

        @Override
        public void childShardFailed(ShardRouting parentShard, ShardRouting childShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.childShardFailed(parentShard, childShard);
            }
        }

        @Override
        public void splitCompleted(ShardRouting removedSplitSource, IndexMetadata indexMetadata) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.splitCompleted(removedSplitSource, indexMetadata);
            }
        }

        @Override
        public void splitFailed(ShardRouting splitSource, IndexMetadata indexMetadata) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.splitFailed(splitSource, indexMetadata);
            }
        }

        @Override
        public void replicaPromoted(ShardRouting replicaShard) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.replicaPromoted(replicaShard);
            }
        }

        @Override
        public void initializedReplicaReinitialized(ShardRouting oldReplica, ShardRouting reinitializedReplica) {
            for (RoutingChangesObserver routingChangesObserver : routingChangesObservers) {
                routingChangesObserver.initializedReplicaReinitialized(oldReplica, reinitializedReplica);
            }
        }
    }
}
