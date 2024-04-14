/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

public class InPlaceShardSplitAllocationDecider extends AllocationDecider {

    public static final String NAME = "in_place_shard_split";

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canRemainDecision(shardRouting, node, allocation);
    }

    public static Decision canRemainDecision(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // If shardRouting is a started parent shard and fact that it exists is sufficient to conclude
        // that it needs to be split.
        if (allocation.metadata().getIndexSafe(shardRouting.index()).isParentShard(shardRouting.shardId()) && shardRouting.started()) {
            return Decision.SPLIT;
        }
        return Decision.ALWAYS;
    }
}
