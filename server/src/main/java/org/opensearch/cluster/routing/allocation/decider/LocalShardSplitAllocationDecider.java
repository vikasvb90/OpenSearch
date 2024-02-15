/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

public class LocalShardSplitAllocationDecider extends AllocationDecider {

    public static final String NAME = "local_shard_split";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canAllocate(shardRouting, null, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo != null && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARD_SPLIT) {
            RecoverySource.LocalShardSplitRecoverySource recoverySource = (RecoverySource.LocalShardSplitRecoverySource) shardRouting
                .recoverySource();
            ShardRouting sourceShard = allocation.routingNodes().activePrimary(recoverySource.getSourceShardId());
            if (sourceShard == null) {
                return allocation.decision(Decision.NO, NAME, "source primary shard [%s] is not active", shardRouting.shardId());
            }
            if (node != null) {
                if (sourceShard.currentNodeId().equals(node.nodeId())) {
                    return allocation.decision(Decision.YES, NAME, "source shard is allocated on this node");
                } else {
                    return allocation.decision(Decision.NO, NAME, "source shard is allocated on another node");
                }
            } else {
                return allocation.decision(Decision.YES, NAME, "source shard is active");
            }
        }

        return super.canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        return canAllocate(shardRouting, node, allocation);
    }
}
