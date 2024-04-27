/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InPlaceShardSplitTargetsListener  implements ReplicationListener {
    private final Map<ShardId, ShardRouting> completedRecoveries;
    private final Map<ShardId, ShardRouting> recoveringShards;
    final ActionListener<Boolean> sourceListener;

    public InPlaceShardSplitTargetsListener(final List<ShardRouting> shardRoutings,
                                            final ActionListener<Boolean> sourceListener) {
        this.completedRecoveries = new HashMap<>();
        this.recoveringShards = new HashMap<>();
        shardRoutings.forEach(shardRouting -> {
            this.recoveringShards.put(shardRouting.shardId(), shardRouting);
        });
        this.sourceListener = sourceListener;
    }


    @Override
    public synchronized void onDone(ReplicationState state) {
        RecoveryState recoveryState = (RecoveryState) state;
        completedRecoveries.put(recoveryState.getShardId(), recoveringShards.get(recoveryState.getShardId()));
        if (recoveringShards.size() == completedRecoveries.size()) {
            sourceListener.onResponse(true);
        }
    }

    @Override
    public synchronized void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
        sourceListener.onFailure(e);
    }
}
