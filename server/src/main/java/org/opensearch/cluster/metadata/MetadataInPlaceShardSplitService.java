/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.plugins.PluginsService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class MetadataInPlaceShardSplitService {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceShardSplitService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final PluginsService pluginsService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final ShardLimitValidator shardLimitValidator;
    private final ClusterManagerTaskThrottler.ThrottlingKey splitShardTaskKey;
    private AwarenessReplicaBalance awarenessReplicaBalance;

    public MetadataInPlaceShardSplitService(
        final Settings settings,
        final ClusterService clusterService,
        final PluginsService pluginsService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final ShardLimitValidator shardLimitValidator,
        final AwarenessReplicaBalance awarenessReplicaBalance
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.pluginsService = pluginsService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.shardLimitValidator = shardLimitValidator;
        this.awarenessReplicaBalance = awarenessReplicaBalance;

        this.splitShardTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.IN_PLACE_SHARD_SPLIT_TASK, true);
    }

    /**
     * TODO: Add comment
     */
    public void split(final InPlaceShardSplitClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        onlySplitShard(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                // TODO: Add wait for active shard.
                listener.onResponse(new ClusterStateUpdateResponse(true));
            } else {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }
        }, listener::onFailure));

    }

    private void onlySplitShard(
        final InPlaceShardSplitClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "in-place-split-shard [" + request.getShardId() + "] of index [" + request.getIndex() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return splitShardTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return applyShardSplitRequest(currentState, request, allocationService::reroute);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] of index [{}] failed to split online",
                            request.getShardId(),
                            request.getIndex()
                        ),
                        e
                    );
                    super.onFailure(source, e);
                }
            }
        );
    }

    public ClusterState applyShardSplitRequest(
        ClusterState currentState,
        InPlaceShardSplitClusterStateUpdateRequest request,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable
    ) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(request.getIndex());
        ShardId sourceShardId = new ShardId(curIndexMetadata.getIndex(), request.getShardId());
        if (curIndexMetadata.getSplitShardsMetadata().getInProgressSplitShardId() != SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS) {
            int inProgressSplitShard = curIndexMetadata.getSplitShardsMetadata().getInProgressSplitShardId();
            throw new IllegalArgumentException("Splitting of shard [" + inProgressSplitShard + "] is already in progress");
        }

        if (curIndexMetadata.getSplitShardsMetadata().isEmptyParentShard(request.getShardId())) {
            throw new IllegalArgumentException("Shard [" + request.getShardId() + "] has already been split.");
        }

        Tuple<Boolean, String> shardSplitSupportedOnPlugins = pluginsService.isShardSplitAllowed(sourceShardId.getIndex());
        if (Boolean.TRUE.equals(shardSplitSupportedOnPlugins.v1()) == false) {
            throw new UnsupportedOperationException("Splitting of shard [" + sourceShardId.id() + "] on index [" +
                sourceShardId.getIndex().getName() + "] is not supported by plugins " + shardSplitSupportedOnPlugins.v2());
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata);

        SplitShardsMetadata.Builder splitMetadataBuilder = new SplitShardsMetadata.Builder(curIndexMetadata.getSplitShardsMetadata());
        splitMetadataBuilder.splitShard(sourceShardId.id(), request.getSplitInto());
        indexMetadataBuilder.splitShardsMetadata(splitMetadataBuilder.build());

        RoutingTable routingTable = routingTableBuilder.build();
        metadataBuilder.put(indexMetadataBuilder);


        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTable).build();
        return rerouteRoutingTable.apply(updatedState, "shard [" + request.getShardId() + "] of index [" + request.getIndex() + "] split");
    }


}
