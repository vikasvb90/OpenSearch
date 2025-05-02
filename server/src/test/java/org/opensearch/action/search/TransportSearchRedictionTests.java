/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.get.MultiGetShardRequest;
import org.opensearch.action.get.MultiGetShardResponse;
import org.opensearch.action.get.TransportGetAction;
import org.opensearch.action.get.TransportMultiGetAction;
import org.opensearch.action.get.TransportShardMultiGetAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.action.termvectors.MultiTermVectorsAction;
import org.opensearch.action.termvectors.MultiTermVectorsItemResponse;
import org.opensearch.action.termvectors.MultiTermVectorsRequestBuilder;
import org.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.opensearch.action.termvectors.MultiTermVectorsShardRequest;
import org.opensearch.action.termvectors.MultiTermVectorsShardResponse;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.action.termvectors.TransportMultiTermVectorsAction;
import org.opensearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.spy;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;


public class TransportSearchRedictionTests extends OpenSearchSingleNodeTestCase {

    private TransportSearchAction createSearchAction(ClusterService clusterService, SearchTransportService searchTransportService) {
        return new TestTransportSearchAction(
                (NodeClient) client(),
            client().threadPool(),
            getInstanceFromNode(CircuitBreakerService.class),
            getInstanceFromNode(TransportService.class),
            getInstanceFromNode(SearchService.class),
            searchTransportService,
            getInstanceFromNode(SearchPhaseController.class),
            clusterService,
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            getInstanceFromNode(NamedWriteableRegistry.class),
            getInstanceFromNode(SearchPipelineService.class),
            getInstanceFromNode(MetricsRegistry.class),
            getInstanceFromNode(SearchRequestOperationsCompositeListenerFactory.class),
            getInstanceFromNode(Tracer.class),
            getInstanceFromNode(TaskResourceTrackingService.class),
            getInstanceFromNode(AllocationService.class)
        );
    }

    private static ClusterState createSplittingShardClusterState(int splitShardId, ClusterService clusterService, AllocationService allocationService) {
        int numberOfChildren = 2;

        ClusterState state = clusterService.state();
        IndexMetadata indexMetadata = state.metadata().index("test");
        SplitShardsMetadata.Builder splitShardsMetadata = new SplitShardsMetadata.Builder(indexMetadata.getSplitShardsMetadata());
        splitShardsMetadata.splitShard(splitShardId, numberOfChildren);
        indexMetadata =  IndexMetadata.builder(state.metadata().index("test")).splitShardsMetadata(splitShardsMetadata.build()).build();
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(indexMetadata, true);
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        return allocationService.reroute(state, "starting-split");
    }

    private static ClusterState splitCompletedState(ClusterState state, AllocationService allocationService) {
        return allocationService.applyStartedShards(state, state.getRoutingNodes().shards(ShardRouting::isSplitTarget));
    }

    private static class TestTransportSearchAction extends TransportSearchAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private final ClusterState initialState;

        static {
            actionName = "indices:data/read/searchtest";
        }

        public TestTransportSearchAction(NodeClient client, ThreadPool threadPool, CircuitBreakerService circuitBreakerService,
                                         TransportService transportService, SearchService searchService,
                                         SearchTransportService searchTransportService, SearchPhaseController searchPhaseController,
                                         ClusterService clusterService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, NamedWriteableRegistry namedWriteableRegistry,
                                         SearchPipelineService searchPipelineService, MetricsRegistry metricsRegistry,
                                         SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory,
                                         Tracer tracer, TaskResourceTrackingService taskResourceTrackingService,
                                         AllocationService allocationService) {
            super(client, threadPool, circuitBreakerService, transportService, searchService, searchTransportService,
                searchPhaseController, clusterService, actionFilters, indexNameExpressionResolver, namedWriteableRegistry,
                searchPipelineService, metricsRegistry, searchRequestOperationsCompositeListenerFactory,
                tracer, taskResourceTrackingService);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            this.initialState = createSplittingShardClusterState(0, clusterService, allocationService);
        }

        protected void registerSearchTransportHandlers(TransportService transportService, SearchService searchService) {
            // Already registered
        }

        protected void waitForSplitCompleteOnStateAndRedrive(ClusterStateObserver observer, Predicate<ClusterState> splitNotActive,
                                                             ActionListener<SearchResponse> delegate,
                                                             BiConsumer<ActionListener<SearchResponse>, ClusterState> localSearchExecutable) {

            ClusterState state = splitCompletedState(initialState, allocationService);
            assertTrue(splitNotActive.test(state));
            ActionListener<SearchResponse> testListener = ActionListener.delegateFailure(delegate, (listener, response) -> {
                listener.onResponse(response);
            });
            localSearchExecutable.accept(testListener, state);
        }

        @Override
        protected ClusterState getClusterState() {
            return initialState;
        }
    }

    private static DfsSearchResult newSearchResult(int shardIndex, ShardSearchContextId contextId, SearchShardTarget target) {
        DfsSearchResult result = new DfsSearchResult(contextId, target, null);
        result.setShardIndex(shardIndex);
        result.termsStatistics(new Term[0], new TermStatistics[0]);
        result.maxDoc(100);
        result.fieldStatistics(new HashMap<>());
        return result;
    }

    public void testSearchRedirectionInQueryFetch() throws InterruptedException, IOException {
        createIndex(
            "test",
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        // Needed to add mapping on index.
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        ensureGreen();

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        SearchTransportService searchTransportService = spy(getInstanceFromNode(SearchTransportService.class));
        int parentShardId = 0;
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));

        ArgumentMatcher<SearchActionListener<SearchPhaseResult>> searchListenerMatcher = argument -> true;
        Mockito.doAnswer(invocation -> {
            ShardSearchRequest shard = invocation.getArgument(1);
            SearchActionListener<DfsSearchResult> listener = invocation.getArgument(3);
            if (shard.shardId().id() == parentShardId) {
                listener.onFailure(new ShardNotFoundException(shard.shardId()));
                return null;
            }
            // We don't hit this path because now DFS_QUERY_THEN_FETCH will execute instead of QUERY_THEN_FETCH
            // because we now have multiple shards.
            listener.onResponse(null);
            return null;
        }).when(searchTransportService).sendExecuteQuery(any(), any(), any(), argThat(searchListenerMatcher));

        ArgumentMatcher<SearchActionListener<QuerySearchResult>> searchQueryListenerMatcher = argument -> true;
        AtomicBoolean dfsCompleted = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            SearchActionListener<QuerySearchResult> listener = invocation.getArgument(3);
            if (dfsCompleted.get() == false) {
                listener.onFailure(new ShardNotFoundException(new ShardId(indexService.index(), parentShardId)));
                return null;
            }
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.topDocs(new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(42, 1.0F) }),
                    2.0F
                ),
                new DocValueFormat[0]);
            listener.onResponse(querySearchResult);
            return null;
        }).when(searchTransportService).sendExecuteQuery(any(), any(), any(), argThat(searchQueryListenerMatcher));

        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchActionListener<DfsSearchResult> listener = (SearchActionListener<DfsSearchResult>) args[3];
            ShardSearchRequest shard = (ShardSearchRequest) args[1];
            if (shard.shardId().id() == parentShardId) {
                listener.onFailure(new ShardNotFoundException(shard.shardId()));
                return null;
            }
            DfsSearchResult dfsSearchResult = newSearchResult(
                shard.shardId().id(),
                new ShardSearchContextId("", 1),
                new SearchShardTarget("node1", shard.shardId(), null, OriginalIndices.NONE)
            );
            dfsCompleted.set(true);
            listener.onResponse(dfsSearchResult);
            return null;
        }).when(searchTransportService).sendExecuteDfs(any(), any(), any(), any());

        TransportSearchAction transportSearchAction = createSearchAction(clusterService, searchTransportService);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequest.indices("test");
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureEx = new AtomicReference<>();
        AtomicReference<SearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                response.set(searchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureEx.set(e);
                latch.countDown();
            }
        };
        SearchTask searchTask = searchRequest.createTask(0, "test", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap());
        transportSearchAction.execute(searchTask, searchRequest, listener);
        latch.await();
        if (failureEx.get() != null) {
            logger.error("Search redirection query test in shard split failed with error ", failureEx.get());
        }
        assertNull(failureEx.get());
        assertEquals(2, response.get().getSuccessfulShards());
    }

    private TransportGetAction createTransportGetAction(ClusterService clusterService, TransportService transportService,
                                                  IndicesService indicesService) {
        return new TestTransportGetAction(
            clusterService,
            transportService,
            indicesService,
            client().threadPool(),
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            getInstanceFromNode(AllocationService.class)
        );
    }

    private static class TestTransportGetAction  extends TransportGetAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private static ClusterState currentState;

        static {
            actionName = "indices:data/read/gettest";
        }

        public TestTransportGetAction(ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      AllocationService allocationService) {
            super(clusterService, transportService, indicesService, threadPool, actionFilters, indexNameExpressionResolver);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            currentState = createSplittingShardClusterState(0, clusterService, allocationService);
        }


        protected void waitForSplitCompletionAndRedrive(ClusterStateObserver observer, ActionListener<GetResponse> delegate,
                                                        TransportSingleShardAction<GetRequest, GetResponse>.InternalRequest internalRequest,
                                                        Exception currentFailure,
                                                        Predicate<ClusterState> splitNotActive) {
            ClusterState state = splitCompletedState(currentState, allocationService);
            assertTrue(splitNotActive.test(state));
            currentState = state;
            publishResponseOnNewClusterState(delegate, internalRequest);
        }

        @Override
        protected ClusterState getClusterState() {
            return currentState;
        }
    }

    public void testSearchRedirectionGetAction() throws InterruptedException, IOException {
        createIndex(
            "test",
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        // Needed to add mapping on index.
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        ensureGreen();

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService searchTransportService = spy(getInstanceFromNode(TransportService.class));
        int parentShardId = 0;
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));

        ArgumentMatcher<TransportResponseHandler<GetResponse>> searchListenerMatcher = argument -> true;
        AtomicBoolean splitFailureThrown = new AtomicBoolean();
        AtomicBoolean splitCompletedInCoordinatorState = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            TransportResponseHandler<GetResponse> getResponseHandler = invocation.getArgument(3);
            if (splitFailureThrown.get() == false) {
                splitFailureThrown.set(true);
                if (splitCompletedInCoordinatorState.get()) {
                    TestTransportGetAction.currentState = splitCompletedState(TestTransportGetAction.currentState,
                        getInstanceFromNode(AllocationService.class));
                }
                TransportException transportException = new RemoteTransportException("", new ShardNotFoundException(new ShardId(indexService.index(), parentShardId)));
                getResponseHandler.handleException(transportException);
                return null;
            }

            final GetResult getResult = new GetResult("test", "1", 0, 1, 0,
                true, new BytesArray("{\"f\":\"v\"}"), null, null);
            getResponseHandler.handleResponse(new GetResponse(getResult));
            return null;
        }).when(searchTransportService).sendRequest(any(), any(), any(), argThat(searchListenerMatcher));

        TransportGetAction transportGetAction = createTransportGetAction(clusterService, searchTransportService, indicesService);
        executeGetRequest(transportGetAction);

        // Now test the scenario when split completed on target node and coordinator also has updated cluster state.
        TestTransportGetAction.currentState = createSplittingShardClusterState(0, clusterService, getInstanceFromNode(AllocationService.class));
        splitCompletedInCoordinatorState.set(true);
        splitFailureThrown.set(false);
        executeGetRequest(transportGetAction);
    }

    private void executeGetRequest(TransportGetAction transportGetAction) throws InterruptedException {
        GetRequest getRequest = new GetRequest();
        getRequest.index("test");
        getRequest.id("_123");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureEx = new AtomicReference<>();
        AtomicReference<GetResponse> response = new AtomicReference<>();
        ActionListener<GetResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(GetResponse searchResponse) {
                response.set(searchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureEx.set(e);
                latch.countDown();
            }
        };

        transportGetAction.execute(getRequest, listener);
        latch.await();
        if (failureEx.get() != null) {
            logger.error("Search redirection get test in shard split failed with error ", failureEx.get());
        }
        assertNull(failureEx.get());
        assertNotNull(response.get().getId());
    }

    private TransportMultiGetAction createTransportMultiGetAction(ClusterService clusterService, TransportService transportService,
                                                                       IndicesService indicesService) {
        TransportShardMultiGetAction shardMultiGetAction = createTransportShardMultiGetAction(
            clusterService, transportService, indicesService
        );

        return new TestTransportMultiGetAction(
            transportService,
            clusterService,
            shardMultiGetAction,
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            getInstanceFromNode(AllocationService.class),
            client().threadPool()
        );
    }

    private static class TestTransportMultiGetAction  extends TransportMultiGetAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private static ClusterState currentState;

        static {
            actionName = "indices:data/read/multigettest";
        }

        public TestTransportMultiGetAction(TransportService transportService, ClusterService clusterService,
                                           TransportShardMultiGetAction shardAction, ActionFilters actionFilters,
                                           IndexNameExpressionResolver resolver, AllocationService allocationService,
                                           ThreadPool threadPool) {
            super(transportService, clusterService, shardAction, actionFilters, resolver, threadPool);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            currentState = createSplittingShardClusterState(0, clusterService, allocationService);
        }

        protected ClusterState getClusterState() {
            return currentState;
        }

        protected void waitForSplitCompleteOnStateAndRedrive(ClusterStateObserver observer, Predicate<ClusterState> splitNotActive,
                                                             ActionListener<MultiGetResponse> listener, Runnable runnable) {
            ClusterState state = splitCompletedState(currentState, allocationService);
            assertTrue(splitNotActive.test(state));
            currentState = state;
            runnable.run();
        }

    }

    private TransportShardMultiGetAction createTransportShardMultiGetAction(ClusterService clusterService, TransportService transportService,
                                                        IndicesService indicesService) {
        return new TestTransportShardMultiGetAction(
            transportService,
            clusterService,
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            indicesService,
            client().threadPool(),
            getInstanceFromNode(AllocationService.class)
        );
    }

    private static class TestTransportShardMultiGetAction  extends TransportShardMultiGetAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private static ClusterState currentState;

        static {
            ACTION_NAME = "indices:data/read/multishardgettest";
        }

        public TestTransportShardMultiGetAction(TransportService transportService, ClusterService clusterService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                IndicesService indicesService, ThreadPool threadPool,
                                                AllocationService allocationService) {
            super(clusterService, transportService, indicesService, threadPool, actionFilters, resolver);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            currentState = createSplittingShardClusterState(0, clusterService, allocationService);
        }

        protected void waitForSplitCompletionAndRedrive(ClusterStateObserver observer, ActionListener<MultiGetShardResponse> delegate,
                                                        TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse>.InternalRequest internalRequest,
                                                        Exception currentFailure,
                                                        Predicate<ClusterState> splitNotActive) {
            ClusterState state = splitCompletedState(currentState, allocationService);
            assertTrue(splitNotActive.test(state));
            currentState = state;
            publishResponseOnNewClusterState(delegate, internalRequest);
        }

        @Override
        protected ClusterState getClusterState() {
            return currentState;
        }
    }

    public void testSearchRedirectionMultiGetAction() throws InterruptedException, IOException {
        createIndex(
            "test",
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        // Needed to add mapping on index.
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        ensureGreen();

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService searchTransportService = spy(getInstanceFromNode(TransportService.class));
        int parentShardId = 0;
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));

        ArgumentMatcher<TransportResponseHandler<MultiGetShardResponse>> searchListenerMatcher = argument -> true;
        AtomicBoolean splitCompletedInCoordinatorState = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            TransportResponseHandler<MultiGetShardResponse> getResponseHandler = invocation.getArgument(3);
            MultiGetShardRequest shardRequest = invocation.getArgument(2);
            if (shardRequest.shardId() == parentShardId) {
                if (splitCompletedInCoordinatorState.get()) {
                    TestTransportShardMultiGetAction.currentState = splitCompletedState(TestTransportShardMultiGetAction.currentState,
                        getInstanceFromNode(AllocationService.class));
                }
                TransportException transportException = new RemoteTransportException("", new ShardNotFoundException(new ShardId(indexService.index(), parentShardId)));
                getResponseHandler.handleException(transportException);
                return null;
            }

            MultiGetShardResponse multiGetShardResponse = new MultiGetShardResponse();
            GetResult getResult = new GetResult("test", "1", 0, 1, 0,
                true, new BytesArray("{\"f\":\"v\"}"), null, null);
            GetResponse getResponse = new GetResponse(getResult);
            multiGetShardResponse.add(0, getResponse);
            getResult = new GetResult("test", "2", 0, 1, 0,
                true, new BytesArray("{}"), null, null);
            getResponse = new GetResponse(getResult);
            multiGetShardResponse.add(1, getResponse);
            getResponseHandler.handleResponse(multiGetShardResponse);
            return null;
        }).when(searchTransportService).sendRequest(any(), any(), any(), argThat(searchListenerMatcher));

        TransportMultiGetAction transportGetAction = createTransportMultiGetAction(clusterService, searchTransportService, indicesService);
        executeMultiGetRequest(transportGetAction);

        // Now test the scenario when split completed on target node and coordinator also has updated cluster state.
        // but stale shard iterator.
        ClusterState splittingState = createSplittingShardClusterState(0, clusterService, getInstanceFromNode(AllocationService.class));
        TestTransportMultiGetAction.currentState = splittingState;
        TestTransportShardMultiGetAction.currentState = splittingState;
        splitCompletedInCoordinatorState.set(true);
        executeMultiGetRequest(transportGetAction);
    }

    private void executeMultiGetRequest(TransportMultiGetAction transportGetAction) throws InterruptedException {
        MultiGetRequest getRequest = new MultiGetRequest();
        getRequest.add("test", "_123");
        getRequest.add("test", "_456");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureEx = new AtomicReference<>();
        AtomicReference<MultiGetResponse> response = new AtomicReference<>();
        ActionListener<MultiGetResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(MultiGetResponse searchResponse) {
                response.set(searchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureEx.set(e);
                latch.countDown();
            }
        };

        transportGetAction.execute(getRequest, listener);
        latch.await();
        if (failureEx.get() != null) {
            logger.error("Search redirection get test in shard split failed with error ", failureEx.get());
        }
        assertNull(failureEx.get());
        for (MultiGetItemResponse itemResponse : response.get().getResponses()) {
            assertNull(itemResponse.getFailure());
            assertFalse(itemResponse.isFailed());
            assertNotNull(itemResponse.getId());
        }
    }

    private TransportMultiTermVectorsAction createTransportMultiTermVectorsAction(ClusterService clusterService, TransportService transportService,
                                                                                 IndicesService indicesService) {
        TransportShardMultiTermsVectorAction shardMultiGetAction = createTransportShardMultiTermsVectorsAction(
            clusterService, transportService, indicesService
        );

        return new TestTransportMultiTermVectorsAction(
            transportService,
            clusterService,
            shardMultiGetAction,
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            getInstanceFromNode(AllocationService.class),
            client().threadPool()
        );
    }

    private static class TestTransportMultiTermVectorsAction  extends TransportMultiTermVectorsAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private static ClusterState currentState;

        static {
            actionName = "indices:data/read/multitermvectorstest";
        }

        public TestTransportMultiTermVectorsAction(TransportService transportService, ClusterService clusterService,
                                           TransportShardMultiTermsVectorAction shardAction, ActionFilters actionFilters,
                                           IndexNameExpressionResolver resolver, AllocationService allocationService,
                                           ThreadPool threadPool) {
            super(transportService, clusterService, shardAction, actionFilters, resolver, threadPool);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            currentState = createSplittingShardClusterState(0, clusterService, allocationService);
        }

        protected ClusterState getClusterState() {
            return currentState;
        }

        protected void waitForSplitCompleteOnStateAndRedrive(ClusterStateObserver observer, Predicate<ClusterState> splitNotActive,
                                                             ActionListener<MultiTermVectorsResponse> listener, Runnable runnable) {
            ClusterState state = splitCompletedState(currentState, allocationService);
            assertTrue(splitNotActive.test(state));
            currentState = state;
            runnable.run();
        }

    }

    private TransportShardMultiTermsVectorAction createTransportShardMultiTermsVectorsAction(ClusterService clusterService, TransportService transportService,
                                                                            IndicesService indicesService) {
        return new TestTransportShardMultiTermVectorsAction(
            transportService,
            clusterService,
            getInstanceFromNode(ActionFilters.class),
            getInstanceFromNode(IndexNameExpressionResolver.class),
            indicesService,
            client().threadPool(),
            getInstanceFromNode(AllocationService.class)
        );
    }

    private static class TestTransportShardMultiTermVectorsAction  extends TransportShardMultiTermsVectorAction {
        private final ClusterService clusterService;
        private final AllocationService allocationService;
        private static ClusterState currentState;

        static {
            ACTION_NAME = "indices:data/read/multishardtermvectortest";
        }

        public TestTransportShardMultiTermVectorsAction(TransportService transportService, ClusterService clusterService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                IndicesService indicesService, ThreadPool threadPool,
                                                AllocationService allocationService) {
            super(clusterService, transportService, indicesService, threadPool, actionFilters, resolver);
            this.clusterService = clusterService;
            this.allocationService = allocationService;
            currentState = createSplittingShardClusterState(0, clusterService, allocationService);
        }

        protected void waitForSplitCompletionAndRedrive(ClusterStateObserver observer, ActionListener<MultiTermVectorsShardResponse> delegate,
                                                        TransportSingleShardAction<MultiTermVectorsShardRequest, MultiTermVectorsShardResponse>.InternalRequest internalRequest,
                                                        Exception currentFailure,
                                                        Predicate<ClusterState> splitNotActive) {
            ClusterState state = splitCompletedState(currentState, allocationService);
            assertTrue(splitNotActive.test(state));
            currentState = state;
            publishResponseOnNewClusterState(delegate, internalRequest);
        }

        @Override
        protected ClusterState getClusterState() {
            return currentState;
        }
    }

    public void testSearchRedirectionMultiTermVectorsAction() throws InterruptedException, IOException {
        createIndex(
            "test",
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        // Needed to add mapping on index.
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        ensureGreen();

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService searchTransportService = spy(getInstanceFromNode(TransportService.class));
        int parentShardId = 0;
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));

        ArgumentMatcher<TransportResponseHandler<MultiTermVectorsResponse>> searchListenerMatcher = argument -> true;
        AtomicBoolean splitCompletedInCoordinatorState = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            TransportResponseHandler<MultiTermVectorsShardResponse> getResponseHandler = invocation.getArgument(3);
            MultiTermVectorsShardRequest shardRequest = invocation.getArgument(2);
            if (shardRequest.shardId() == parentShardId) {
                if (splitCompletedInCoordinatorState.get()) {
                    TestTransportShardMultiTermVectorsAction.currentState = splitCompletedState(TestTransportShardMultiTermVectorsAction.currentState,
                        getInstanceFromNode(AllocationService.class));
                }
                TransportException transportException = new RemoteTransportException("", new ShardNotFoundException(new ShardId(indexService.index(), parentShardId)));
                getResponseHandler.handleException(transportException);
                return null;
            }

            MultiTermVectorsShardResponse shardResponse = new MultiTermVectorsShardResponse();
            shardResponse.add(0, new TermVectorsResponse("test", "_123"));
            shardResponse.add(1, new TermVectorsResponse("test", "_456"));
            getResponseHandler.handleResponse(shardResponse);
            return null;
        }).when(searchTransportService).sendRequest(any(), any(), any(), argThat(searchListenerMatcher));

        TransportMultiTermVectorsAction transportGetAction = createTransportMultiTermVectorsAction(clusterService, searchTransportService, indicesService);
        executeMultiTermVectorRequest(transportGetAction);

        // Now test the scenario when split completed on target node and coordinator also has updated cluster state.
        ClusterState splittingState = createSplittingShardClusterState(0, clusterService, getInstanceFromNode(AllocationService.class));
        TestTransportMultiTermVectorsAction.currentState = splittingState;
        TestTransportShardMultiTermVectorsAction.currentState = splittingState;
        splitCompletedInCoordinatorState.set(true);
        executeMultiTermVectorRequest(transportGetAction);
    }

    private void executeMultiTermVectorRequest(TransportMultiTermVectorsAction transportGetAction) throws InterruptedException {
        final MultiTermVectorsRequestBuilder request = new MultiTermVectorsRequestBuilder(client(), MultiTermVectorsAction.INSTANCE);
        request.add(new TermVectorsRequest("test", "1"));
        request.add(new TermVectorsRequest("test", "2"));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failureEx = new AtomicReference<>();
        AtomicReference<MultiTermVectorsResponse> response = new AtomicReference<>();
        ActionListener<MultiTermVectorsResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(MultiTermVectorsResponse searchResponse) {
                response.set(searchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureEx.set(e);
                latch.countDown();
            }
        };

        transportGetAction.execute(request.request(), listener);
        latch.await();
        if (failureEx.get() != null) {
            logger.error("Search redirection get test in shard split failed with error ", failureEx.get());
        }
        assertNull(failureEx.get());
        for (MultiTermVectorsItemResponse itemResponse : response.get().getResponses()) {
            assertNull(itemResponse.getFailure());
            assertFalse(itemResponse.isFailed());
            assertNotNull(itemResponse.getId());
        }
    }
}
