/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InPlaceShardSplitRecoveryListener implements ReplicationListener {

    private final Map<ShardId, ShardRouting> recoveringShards;
    private final IndicesClusterStateService indicesClusterStateService;
    private final ShardRouting sourceShard;
    private final long primaryTerm;

    public InPlaceShardSplitRecoveryListener (
        final List<ShardRouting> shardRoutings,
        IndicesClusterStateService indicesClusterStateService,
        final ShardRouting sourceShard,
        final long primaryTerm
    ) {
        this.sourceShard = sourceShard;
        this.recoveringShards = new HashMap<>();
        this.primaryTerm = primaryTerm;
        shardRoutings.forEach(shardRouting -> {
            this.recoveringShards.put(shardRouting.shardId(), shardRouting);
        });
        this.indicesClusterStateService = indicesClusterStateService;
    }

    /**
     * Handle recover done event for in-place shard split recovery.
     */
    @Override
    public synchronized void onDone(ReplicationState state) {
        indicesClusterStateService.handleChildRecoveriesDone(sourceShard, primaryTerm,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE);
    }

    /**
     * Handle recover failure event for in-place shard split recovery.
     */
    @Override
    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
        indicesClusterStateService.handleChildRecoveriesFailure(sourceShard, sendShardFailure, e);
    }

}
