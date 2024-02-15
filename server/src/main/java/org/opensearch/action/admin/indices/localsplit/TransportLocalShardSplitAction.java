/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.localsplit;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataLocalShardSplitService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportLocalShardSplitAction extends TransportClusterManagerNodeAction<LocalShardSplit, LocalShardSplitResponse> {

    private final MetadataLocalShardSplitService metadataLocalShardSplitService;
    private final Client client;

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected LocalShardSplitResponse read(StreamInput in) throws IOException {
        return new LocalShardSplitResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LocalShardSplit request, ClusterState state) {
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
    public TransportLocalShardSplitAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataLocalShardSplitService metadataLocalShardSplitService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        this(
            LocalShardSplitAction.NAME,
            transportService,
            clusterService,
            threadPool,
            metadataLocalShardSplitService,
            actionFilters,
            indexNameExpressionResolver,
            client
        );
    }

    protected TransportLocalShardSplitAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataLocalShardSplitService metadataLocalShardSplitService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, LocalShardSplit::new, indexNameExpressionResolver);
        this.metadataLocalShardSplitService = metadataLocalShardSplitService;
        this.client = client;
    }

    @Override
    protected void clusterManagerOperation(
        final LocalShardSplit localShardSplit,
        final ClusterState clusterState,
        final ActionListener<LocalShardSplitResponse> listener
    ) {
        final String index = indexNameExpressionResolver.resolveDateMathExpression(localShardSplit.getIndex());
        ShardId shardId = clusterState.getRoutingTable().shardRoutingTable(index, localShardSplit.getShardId()).shardId();
        LocalShardSplitClusterStateUpdateRequest updateRequest = prepareLocalShardSplitRequest(localShardSplit, shardId, clusterState);
        metadataLocalShardSplitService.splitLocalShard(
            updateRequest,
            ActionListener.map(
                listener,
                response -> new LocalShardSplitResponse(response.isAcknowledged(), index, shardId.id(), localShardSplit.getSplitInto())
            )
        );
    }

    private LocalShardSplitClusterStateUpdateRequest prepareLocalShardSplitRequest(
        LocalShardSplit localShardSplit,
        ShardId shardId,
        final ClusterState state
    ) {
        final IndexMetadata metadata = state.metadata().index(localShardSplit.getIndex());
        if (metadata == null) {
            throw new IndexNotFoundException(localShardSplit.getIndex());
        }

        if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(metadata.getSettings()) == true) {
            throw new IllegalArgumentException(
                "local shard split cannot be triggered on index [" + shardId.getIndexName() + "] as it has read-only block"
            );
        }

        return new LocalShardSplitClusterStateUpdateRequest(
            "local_shard_split",
            shardId.getIndexName(),
            shardId.id(),
            localShardSplit.getSplitInto()
        ).masterNodeTimeout(localShardSplit.clusterManagerNodeTimeout()).ackTimeout(localShardSplit.ackTimeout());
    }
}
