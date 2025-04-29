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

package org.opensearch.action.termvectors;

import org.opensearch.action.PrimaryShardSplitException;
import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Performs the multi term get operation.
 *
 * @opensearch.internal
 */
public class TransportMultiTermVectorsAction extends HandledTransportAction<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    private final ClusterService clusterService;
    private final TransportShardMultiTermsVectorAction shardAction;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    protected static String actionName = MultiTermVectorsAction.NAME;
    private final ThreadPool threadPool;

    @Inject
    public TransportMultiTermVectorsAction(
        TransportService transportService,
        ClusterService clusterService,
        TransportShardMultiTermsVectorAction shardAction,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ThreadPool threadPool
    ) {
        super(actionName, transportService, actionFilters, MultiTermVectorsRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
    }

    protected ClusterState getClusterState() {
        return clusterService.state();
    }

    @Override
    protected void doExecute(Task task, final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        ClusterState clusterState = getClusterState();

        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicArray<MultiTermVectorsItemResponse> responses = new AtomicArray<>(request.requests.size());

        Map<ShardId, MultiTermVectorsShardRequest> shardRequests = new HashMap<>();
        for (int i = 0; i < request.requests.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            termVectorsRequest.routing(
                clusterState.metadata().resolveIndexRouting(termVectorsRequest.routing(), termVectorsRequest.index())
            );
            if (!clusterState.metadata().hasConcreteIndex(termVectorsRequest.index())) {
                responses.set(
                    i,
                    new MultiTermVectorsItemResponse(
                        null,
                        new MultiTermVectorsResponse.Failure(
                            termVectorsRequest.index(),
                            termVectorsRequest.id(),
                            new IndexNotFoundException(termVectorsRequest.index())
                        )
                    )
                );
                continue;
            }
            String concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, termVectorsRequest).getName();
            if (termVectorsRequest.routing() == null && clusterState.getMetadata().routingRequired(concreteSingleIndex)) {
                responses.set(
                    i,
                    new MultiTermVectorsItemResponse(
                        null,
                        new MultiTermVectorsResponse.Failure(
                            concreteSingleIndex,
                            termVectorsRequest.id(),
                            new RoutingMissingException(concreteSingleIndex, termVectorsRequest.id())
                        )
                    )
                );
                continue;
            }
            ShardId shardId = clusterService.operationRouting()
                .shardId(clusterState, concreteSingleIndex, termVectorsRequest.id(), termVectorsRequest.routing());
            MultiTermVectorsShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiTermVectorsShardRequest(shardId.getIndexName(), shardId.id());
                shardRequest.preference(request.preference);
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, termVectorsRequest);
        }

        if (shardRequests.size() == 0) {
            // only failures..
            listener.onResponse(new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()])));
        }

        executeShardAction(wrapWithSplitExHandler(task, request, listener, clusterState), responses, shardRequests);
    }

    protected ActionListener<MultiTermVectorsResponse> wrapWithSplitExHandler(Task task, MultiTermVectorsRequest request,
                                                                      ActionListener<MultiTermVectorsResponse> listener,
                                                                      ClusterState clusterState) {
        return ActionListener.delegateResponse(listener, (delegatedListener, ex) -> {
            if (ex instanceof PrimaryShardSplitException) {
                ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null,
                    logger, threadPool.getThreadContext());
                PrimaryShardSplitException splitException = (PrimaryShardSplitException) ex;
                waitForSplitCompleteOnStateAndRedrive(
                    observer,
                    InPlaceShardSplitRecoveryService.splitNotActivePredicate(splitException.getShardId()),
                    listener,
                    () -> this.doExecute(task, request, listener)
                );
                return;
            }
            listener.onFailure(ex);
        });
    }

    protected void waitForSplitCompleteOnStateAndRedrive(ClusterStateObserver observer, Predicate<ClusterState> splitNotActive,
                                                         ActionListener<MultiTermVectorsResponse> listener, Runnable runnable) {
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
        ActionListener<MultiTermVectorsResponse> listener,
        AtomicArray<MultiTermVectorsItemResponse> responses,
        Map<ShardId, MultiTermVectorsShardRequest> shardRequests
    ) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiTermVectorsShardRequest shardRequest : shardRequests.values()) {
            shardAction.execute(shardRequest, new ActionListener<MultiTermVectorsShardResponse>() {
                private final AtomicReference<PrimaryShardSplitException> shardSplitEx = new AtomicReference<>();

                @Override
                public void onResponse(MultiTermVectorsShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        responses.set(
                            response.locations.get(i),
                            new MultiTermVectorsItemResponse(response.responses.get(i), response.failures.get(i))
                        );
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
                        TermVectorsRequest termVectorsRequest = shardRequest.requests.get(i);
                        responses.set(
                            shardRequest.locations.get(i),
                            new MultiTermVectorsItemResponse(
                                null,
                                new MultiTermVectorsResponse.Failure(shardRequest.index(), termVectorsRequest.id(), e)
                            )
                        );
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
                    listener.onResponse(
                        new MultiTermVectorsResponse(responses.toArray(new MultiTermVectorsItemResponse[responses.length()]))
                    );
                }
            });
        }
    }
}
