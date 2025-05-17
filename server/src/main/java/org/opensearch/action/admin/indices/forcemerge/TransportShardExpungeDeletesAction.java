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

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * ForceMerge index/indices action.
 *
 * @opensearch.internal
 */
public class TransportShardExpungeDeletesAction extends TransportSingleShardAction<ForceExpungeDeletesShardRequest, ForceExpungeDeletesShardResponse> {

    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

    @Inject
    public TransportShardExpungeDeletesAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool
    ) {
        super(
            ForceExpungeDeletesShardAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ForceExpungeDeletesShardRequest::new,
            ThreadPool.Names.FORCE_MERGE
        );
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        Index index = indexNameExpressionResolver.concreteSingleIndex(clusterService.state(), request.request());
        ShardId shardId = new ShardId(index, request.request().getShardId());
        ShardRouting routing = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        return new PlainShardIterator(shardId, Collections.singletonList(routing));
    }

    @Override
    protected ForceExpungeDeletesShardResponse shardOperation(ForceExpungeDeletesShardRequest request, ShardId shardId) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        indexShard.onlyExpungeDeletes(request.getExpungeDeletesUUID());
        return new ForceExpungeDeletesShardResponse(shardId, request.getExpungeDeletesUUID());
    }

    @Override
    protected Writeable.Reader<ForceExpungeDeletesShardResponse> getResponseReader() {
        return ForceExpungeDeletesShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(ForceExpungeDeletesShardRequest request) {
        return true;
    }
}
