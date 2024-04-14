/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataInPlaceShardSplitService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportInPlaceShardSplitAction extends TransportClusterManagerNodeAction<
    InPlaceShardSplitRequest,
    InPlaceShardSplitResponse> {

    private final MetadataInPlaceShardSplitService metadataInPlaceShardSplitService;
    private final Client client;

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected InPlaceShardSplitResponse read(StreamInput in) throws IOException {
        return new InPlaceShardSplitResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(InPlaceShardSplitRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        clusterBlockException = state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getIndex());
        if (clusterBlockException != null) {
            return clusterBlockException;
        }

        clusterBlockException = state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.getIndex());
        if (clusterBlockException != null) {
            return clusterBlockException;
        }

        return null;
    }

    @Inject
    public TransportInPlaceShardSplitAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataInPlaceShardSplitService metadataInPlaceShardSplitService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        this(
            InPlaceShardSplitAction.NAME,
            transportService,
            clusterService,
            threadPool,
            metadataInPlaceShardSplitService,
            actionFilters,
            indexNameExpressionResolver,
            client
        );
    }

    protected TransportInPlaceShardSplitAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataInPlaceShardSplitService metadataInPlaceShardSplitService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            actionName,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            InPlaceShardSplitRequest::new,
            indexNameExpressionResolver
        );
        this.metadataInPlaceShardSplitService = metadataInPlaceShardSplitService;
        this.client = client;
    }

    @Override
    protected void clusterManagerOperation(
        final InPlaceShardSplitRequest inPlaceShardSplitRequest,
        final ClusterState clusterState,
        final ActionListener<InPlaceShardSplitResponse> listener
    ) {
        final String index = indexNameExpressionResolver.resolveDateMathExpression(inPlaceShardSplitRequest.getIndex());
        ShardId shardId = clusterState.getRoutingTable().shardRoutingTable(index, inPlaceShardSplitRequest.getShardId()).shardId();
        InPlaceShardSplitClusterStateUpdateRequest updateRequest = prepareInPlaceShardSplitRequest(
            inPlaceShardSplitRequest,
            shardId,
            clusterState
        );
        metadataInPlaceShardSplitService.split(
            updateRequest,
            ActionListener.map(
                listener,
                response -> new InPlaceShardSplitResponse(
                    response.isAcknowledged(),
                    index,
                    shardId.id(),
                    inPlaceShardSplitRequest.getSplitInto()
                )
            )
        );
    }

    private InPlaceShardSplitClusterStateUpdateRequest prepareInPlaceShardSplitRequest(
        InPlaceShardSplitRequest inPlaceShardSplitRequest,
        ShardId shardId,
        final ClusterState state
    ) {
        final IndexMetadata metadata = state.metadata().index(inPlaceShardSplitRequest.getIndex());
        if (metadata == null) {
            throw new IndexNotFoundException(inPlaceShardSplitRequest.getIndex());
        }

        if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(metadata.getSettings()) == true) {
            throw new IllegalArgumentException(
                "in place shard split cannot be triggered on index [" + shardId.getIndexName() + "] as it has read-only block"
            );
        }

        return new InPlaceShardSplitClusterStateUpdateRequest(
            "in_place_shard_split_request",
            shardId.getIndexName(),
            shardId.id(),
            inPlaceShardSplitRequest.getSplitInto()
        ).masterNodeTimeout(inPlaceShardSplitRequest.clusterManagerNodeTimeout()).ackTimeout(inPlaceShardSplitRequest.ackTimeout());
    }
}
