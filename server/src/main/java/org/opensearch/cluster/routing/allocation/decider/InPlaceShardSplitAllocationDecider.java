/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

public class InPlaceShardSplitAllocationDecider extends AllocationDecider {

    public static final String NAME = "in_place_shard_split";

    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.isSplitTarget() || shardRouting.splitting()) {
            return Decision.NO;
        }
        return Decision.ALWAYS;
    }

    public Decision canMoveAway(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.isSplitTarget() || shardRouting.splitting()) {
            return Decision.NO;
        }
        return Decision.ALWAYS;
    }
}
