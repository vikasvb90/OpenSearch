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
import org.opensearch.action.admin.indices.localsplit.LocalShardSplitClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;

public class MetadataLocalShardSplitService {
    private static final Logger logger = LogManager.getLogger(MetadataLocalShardSplitService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final ShardLimitValidator shardLimitValidator;
    private final ClusterManagerTaskThrottler.ThrottlingKey splitShardTaskKey;
    private AwarenessReplicaBalance awarenessReplicaBalance;

    public MetadataLocalShardSplitService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final ShardLimitValidator shardLimitValidator,
        final AwarenessReplicaBalance awarenessReplicaBalance
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.shardLimitValidator = shardLimitValidator;
        this.awarenessReplicaBalance = awarenessReplicaBalance;

        splitShardTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.LOCAL_SHARD_SPLIT_TASK, true);
    }

    /**
     * TODO: Add comment
     */
    public void splitLocalShard(
        final LocalShardSplitClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
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
        final LocalShardSplitClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "online-split-shard [" + request.getShardId() + "] of index [" + request.getIndex() + "], cause [" + request.cause() + "]",
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
        LocalShardSplitClusterStateUpdateRequest request,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable
    ) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(request.getIndex());
        if (!curIndexMetadata.getNewShardRecoveries().isEmpty()) {
            throw new IllegalArgumentException("New local shard recoveries are already in progress");
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        ShardId sourceShardId = currentState.getRoutingTable().shardRoutingTable(request.getIndex(), request.getShardId()).shardId();
        routingTableBuilder.addSplitChildShards(request.getSplitInto(), request.getIndex(), sourceShardId, curIndexMetadata);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata);
        RoutingTable routingTable = routingTableBuilder.build();

        for (IndexShardRoutingTable childShard : routingTable.shardRoutingTable(sourceShardId).getChildShards()) {
            NewRecoveringShardMetadata.Builder recoveringShardMetadata = new NewRecoveringShardMetadata.Builder(childShard.shardId().id());
            recoveringShardMetadata.sourceShardId(sourceShardId.id());
            recoveringShardMetadata.inSyncAllocationIds(Collections.emptySet());
            indexMetadataBuilder.putNewRecoveringShardMetadata(recoveringShardMetadata.build());
        }
        metadataBuilder.put(indexMetadataBuilder);

        ClusterState updatedState = ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .routingTable(routingTable)
            .build();
        return rerouteRoutingTable.apply(updatedState, "shard [" + request.getShardId() + "] of index [" + request.getIndex() + "] split");
    }

}
