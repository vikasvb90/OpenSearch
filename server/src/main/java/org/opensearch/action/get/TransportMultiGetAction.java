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

package org.opensearch.action.get;

import org.opensearch.action.PrimaryShardSplitException;
import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Perform the multi get action.
 *
 * @opensearch.internal
 */
public class TransportMultiGetAction extends HandledTransportAction<MultiGetRequest, MultiGetResponse> {

    private final ClusterService clusterService;
    private final TransportShardMultiGetAction shardAction;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    protected static String actionName = MultiGetAction.NAME;
    private final ThreadPool threadPool;

    @Inject
    public TransportMultiGetAction(
        TransportService transportService,
        ClusterService clusterService,
        TransportShardMultiGetAction shardAction,
        ActionFilters actionFilters,
        IndexNameExpressionResolver resolver,
        ThreadPool threadPool
    ) {
        super(actionName, transportService, actionFilters, MultiGetRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = resolver;
        this.threadPool = threadPool;
    }

    protected static boolean shouldForcePrimaryRouting(Metadata metadata, boolean realtime, String preference, String indexName) {
        return metadata.isSegmentReplicationEnabled(indexName) && realtime && preference == null;
    }

    protected ClusterState getClusterState() {
        return clusterService.state();
    }

    @Override
    protected void doExecute(Task task, final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        ClusterState clusterState = getClusterState();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiGetItemResponse> responses = new AtomicArray<>(request.items.size());
        final Map<ShardId, MultiGetShardRequest> shardRequests = new HashMap<>();

        for (int i = 0; i < request.items.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);

            String concreteSingleIndex;
            try {
                concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, item).getName();

                item.routing(clusterState.metadata().resolveIndexRouting(item.routing(), item.index()));
                if ((item.routing() == null) && (clusterState.getMetadata().routingRequired(concreteSingleIndex))) {
                    responses.set(
                        i,
                        newItemFailure(concreteSingleIndex, item.id(), new RoutingMissingException(concreteSingleIndex, item.id()))
                    );
                    continue;
                }
            } catch (Exception e) {
                responses.set(i, newItemFailure(item.index(), item.id(), e));
                continue;
            }

            ShardId shardId = clusterService.operationRouting()
                .getShards(clusterState, concreteSingleIndex, item.id(), item.routing(), null)
                .shardId();

            MultiGetShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                if (shouldForcePrimaryRouting(clusterState.getMetadata(), request.realtime(), request.preference(), concreteSingleIndex)) {
                    request.preference(Preference.PRIMARY.type());
                }
                shardRequest = new MultiGetShardRequest(request, shardId.getIndexName(), shardId.getId());
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, item);
        }

        if (shardRequests.isEmpty()) {
            // only failures..
            listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
        }
        executeShardAction(wrapWithSplitExHandler(task, request, listener, clusterState), responses, shardRequests);
    }

    protected ActionListener<MultiGetResponse> wrapWithSplitExHandler(Task task, MultiGetRequest request,
                                                                      ActionListener<MultiGetResponse> listener,
                                                                      ClusterState clusterState) {
        return ActionListener.delegateResponse(listener, (delegatedListener, ex) -> {
            if (ex instanceof PrimaryShardSplitException) {
                ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null,
                    logger, threadPool.getThreadContext());
                PrimaryShardSplitException splitException = (PrimaryShardSplitException) ex;
                waitForSplitCompleteOnStateAndRedrive(
                    observer,
                    SplitShardsMetadata.splitNotActivePredicate(splitException.getShardId()),
                    listener,
                    () -> this.doExecute(task, request, listener)
                );
                return;
            }
            listener.onFailure(ex);
        });
    }

    protected void waitForSplitCompleteOnStateAndRedrive(ClusterStateObserver observer, Predicate<ClusterState> splitNotActive,
                                                         ActionListener<MultiGetResponse> listener, Runnable runnable) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                runnable.run();
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                throw new AssertionError("Cannot happen: there is not timeout");
            }
        }, splitNotActive);
    }

    protected void executeShardAction(
        ActionListener<MultiGetResponse> listener,
        AtomicArray<MultiGetItemResponse> responses,
        Map<ShardId, MultiGetShardRequest> shardRequests
    ) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiGetShardRequest shardRequest : shardRequests.values()) {
            shardAction.execute(shardRequest, new ActionListener<MultiGetShardResponse>() {
                private final AtomicReference<PrimaryShardSplitException> shardSplitEx = new AtomicReference<>();

                @Override
                public void onResponse(MultiGetShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        MultiGetItemResponse itemResponse = new MultiGetItemResponse(response.responses.get(i), response.failures.get(i));
                        responses.set(response.locations.get(i), itemResponse);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof PrimaryShardSplitException) {
                        shardSplitEx.set((PrimaryShardSplitException) e);
                    }
                    // create failures for all relevant requests
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        MultiGetRequest.Item item = shardRequest.items.get(i);
                        responses.set(shardRequest.locations.get(i), newItemFailure(shardRequest.index(), item.id(), e));
                    }
                    if (counter.decrementAndGet() == 0) {
                        if (shardSplitEx.get() != null) {
                            listener.onFailure(shardSplitEx.get());
                        } else {
                            finishHim();
                        }
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
                }
            });
        }
    }

    private static MultiGetItemResponse newItemFailure(String index, String id, Exception exception) {
        return new MultiGetItemResponse(null, new MultiGetResponse.Failure(index, id, exception));
    }
}
