/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.Version;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.BulkShardResponse;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardRecoveryContext;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryListener;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryService;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoverySourceHandler;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryTargetHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class InPlaceShardSplitRecoverySourceHandlerTests extends OpenSearchTestCase {

    static class TestSplitResources {
        private TestShardUtils testShardUtils;
        private TestClusterService clusterService;
        private IndicesService indicesService;
        private DiscoveryNode localNode;

        public void setUp() throws Exception {
            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            testShardUtils = new TestShardUtils();
            testShardUtils.setUp();
            String parentNode = "parent-node-" + randomAlphaOfLength(10);
            ClusterApplierService clusterApplierService = new ClusterApplierService(
                parentNode,
                Settings.EMPTY,
                clusterSettings,
                mock(ThreadPool.class)
            );
            Metadata metadata = Metadata.builder().build();
            localNode = new DiscoveryNode(parentNode, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build();
            ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .nodes(discoveryNodes)
                .metadata(metadata).build();
            clusterApplierService.setInitialState(clusterState);
            clusterService = new TestClusterService(
                Settings.EMPTY,
                clusterSettings,
                mock(ClusterManagerService.class),
                clusterApplierService
            );

            indicesService = mock(IndicesService.class);
            when(indicesService.clusterService()).thenReturn(clusterService);
        }

        public void tearDown() throws Exception {
            testShardUtils.tearDown();
        }
    }


    static class TestInPlaceShardSplitRecoveryTargetHandler extends InPlaceShardSplitRecoveryTargetHandler {

        public TestInPlaceShardSplitRecoveryTargetHandler(List<IndexShard> indexShards, DiscoveryNode sourceNode, CancellableThreads cancellableThreads, List<InPlaceShardRecoveryContext> recoveryContexts, Set<String> childShardsAllocationIds, IndexShard sourceShard) {
            super(indexShards, sourceNode, cancellableThreads, recoveryContexts, childShardsAllocationIds, sourceShard);
        }
    }

    static class TestInPlaceShardSplitRecoverySourceHandler extends InPlaceShardSplitRecoverySourceHandler {
        private final List<InPlaceShardRecoveryContext> recoveryContexts;
        private final IndexShard sourceShard;
        private final TestShardSplitParams testShardSplitParams;
        private final TestShardUtils testShardUtils;
        private ActionListener<RecoveryResponse> recoveryResponseListener;
        private final Consumer<IndexShard> failParent;
        private final Consumer<ShardId> cancelRecovery;

        public TestInPlaceShardSplitRecoverySourceHandler(
            IndexShard sourceShard, InPlaceShardSplitRecoveryTargetHandler recoveryTarget, StartRecoveryRequest request,
            int fileChunkSizeInBytes, int maxConcurrentFileChunks, int maxConcurrentOperations,
            CancellableThreads cancellableThreads, List<InPlaceShardRecoveryContext> recoveryContexts,
            Set<String> childShardsAllocationIds, InPlaceShardSplitRecoveryListener replicationListener,
            IndexMetadata indexMetadata, Consumer<ShardId> onSync, TestShardSplitParams testShardSplitParams,
            TestShardUtils testShardUtils, Consumer<IndexShard> failParent, Consumer<ShardId> cancelRecovery) {

            super(sourceShard, recoveryTarget, request, fileChunkSizeInBytes, maxConcurrentFileChunks,
                maxConcurrentOperations, cancellableThreads, recoveryContexts, childShardsAllocationIds,
                replicationListener, indexMetadata, onSync);

            this.sourceShard = sourceShard;
            this.recoveryContexts = recoveryContexts;
            this.testShardSplitParams = testShardSplitParams;
            this.testShardUtils = testShardUtils;
            this.failParent = failParent;
            this.cancelRecovery = cancelRecovery;
        }

        @Override
        public void recoverToTarget(ActionListener<RecoveryResponse> recoveryResponseListener) {
            this.recoveryResponseListener = recoveryResponseListener;
            if (testShardSplitParams.initiateSplitImmediately == true) {
                initiateRecovery();
            }
        }

        public void initiateRecovery() {
            super.recoverToTarget(this.recoveryResponseListener);
        }

        @Override
        protected void initiateTracking() {
            if (testShardSplitParams.testRecoveryFailure && randomBoolean()) {
                throw new RuntimeException("Injected failure");
            } else if (testShardSplitParams.testParentShardFailure && randomBoolean()) {
                failParent.accept(parentShard);
            } else if (testShardSplitParams.testRecoveryCancelled && randomBoolean()) {
                cancelRecovery.accept(parentShard.shardId());
            }

            if (testShardSplitParams.indexDocsForTranslogReplay) {
                int numDocs = scaledRandomIntBetween(2000, 5000);
                try {
                    indexDocs(sourceShard, testShardUtils, numDocs, testShardSplitParams);
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
            super.initiateTracking();
            Set<ShardRouting> replicatedRoutings = new HashSet<>(sourceShard.getReplicationGroup().getReplicationTargets());
            List<IndexShard> childShards = new ArrayList<>();
            recoveryContexts.forEach(context -> {
                assertTrue(replicatedRoutings.contains(context.getIndexShard().routingEntry()));
                childShards.add(context.getIndexShard());
            });
            testShardUtils.addReplicatingShards(childShards);
        }

        @Override
        protected long cacheMaxSequenceNumber() {
            long maxSequenceNumber = super.cacheMaxSequenceNumber();
            if (testShardSplitParams.indexDocsForTranslogReplay) {
                int numDocs = scaledRandomIntBetween(2000, 5000);
                try {
                    indexDocs(sourceShard, testShardUtils, numDocs, testShardSplitParams);
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return maxSequenceNumber;
        }

        @Override
        protected void markAllocationIdAsInSync(List<SendSnapshotResult> sendSnapshotResults) {
            if (testShardSplitParams.testRecoveryFailure) {
                throw new RuntimeException("Injected failure");
            }  else if (testShardSplitParams.testParentShardFailure) {
                failParent.accept(parentShard);
                // Don't return from here. Let's test whether recovery fails while shard failure is accepted.
            } else if (testShardSplitParams.testRecoveryCancelled ) {
                cancelRecovery.accept(parentShard.shardId());
                // Don't return from here. Let's test whether recovery fails while recovery was cancelled.
            }

            if (testShardSplitParams.continueIndexingAfterReplay) {
                try {
                    indexDocs(sourceShard, testShardUtils, randomIntBetween(2000, 5000), testShardSplitParams);
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
            super.markAllocationIdAsInSync(sendSnapshotResults);
        }

        static class TestOngoingRecoveries extends InPlaceShardSplitRecoveryService.OngoingRecoveries {
            TestShardSplitParams testShardSplitParams;
            TestShardUtils testShardUtils;
            Consumer<IndexShard> failParent;
            Consumer<ShardId> cancelRecovery;

            public TestOngoingRecoveries(Lifecycle lifecycle, IndicesService indicesService, RecoverySettings recoverySettings) {
                super(lifecycle, indicesService, recoverySettings);
            }

            @Override
            protected InPlaceShardSplitRecoverySourceHandler createSourceHandler(
                IndexShard sourceShard,
                InPlaceShardSplitRecoveryTargetHandler targetHandler,
                StartRecoveryRequest request,
                CancellableThreads cancellableThreads,
                List<InPlaceShardRecoveryContext> recoveryContexts,
                Set<String> childShardsAllocationIds,
                InPlaceShardSplitRecoveryListener replicationListener,
                IndexMetadata indexMetadata
            ) {
                return new TestInPlaceShardSplitRecoverySourceHandler(sourceShard, targetHandler,
                    request, Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                    recoverySettings.getMaxConcurrentFileChunks(), recoverySettings.getMaxConcurrentOperations(),
                    cancellableThreads, recoveryContexts, childShardsAllocationIds, replicationListener, indexMetadata, onSync,
                    testShardSplitParams, testShardUtils, failParent, cancelRecovery);
            }

            @Override
            protected InPlaceShardSplitRecoveryTargetHandler createSplitTargetHandler(
                List<IndexShard> targetShards,
                DiscoveryNode node,
                CancellableThreads cancellableThreads,
                List<InPlaceShardRecoveryContext> recoveryContexts,
                Set<String> childShardsAllocationIds,
                IndexShard sourceShard
            ) {
                return new TestInPlaceShardSplitRecoveryTargetHandler(targetShards,
                    node, cancellableThreads, recoveryContexts, childShardsAllocationIds ,sourceShard);
            }

            public void triggerSplit(ShardId parentShardId) {
                TestInPlaceShardSplitRecoverySourceHandler sourceHandler = (TestInPlaceShardSplitRecoverySourceHandler)
                    recoveries.get(parentShardId).getSourceHandler();
                sourceHandler.initiateRecovery();
            }

            public boolean isRecoveryOngoing(ShardId parentShardId) {
                return isRecoveryOfShardOnGoing(parentShardId);
            }
        }
    }

    static class TestInPlaceShardSplitRecoveryService extends InPlaceShardSplitRecoveryService {
        private TestInPlaceShardSplitRecoverySourceHandler.TestOngoingRecoveries ongoingRecoveries;

        public TestInPlaceShardSplitRecoveryService(RecoverySettings recoverySettings,
                                                    TestShardSplitParams testShardSplitParams) {
            super(testShardSplitParams.testSplitResources.indicesService, recoverySettings);
            this.ongoingRecoveries.testShardUtils = testShardSplitParams.testSplitResources.testShardUtils;
            this.ongoingRecoveries.failParent = (parentShard) -> beforeIndexShardClosed(null, parentShard, null);
            this.ongoingRecoveries.cancelRecovery = (shardId) -> cancelRecovery(shardId);
            this.ongoingRecoveries.testShardSplitParams = testShardSplitParams;
        }

        @Override
        protected OngoingRecoveries createOngoingRecoveries(Lifecycle lifecycle, IndicesService indicesService, RecoverySettings recoverySettings) {
            this.ongoingRecoveries = new TestInPlaceShardSplitRecoverySourceHandler.TestOngoingRecoveries(
                lifecycle, indicesService, recoverySettings
            );
            return this.ongoingRecoveries;
        }

        public void triggerSplit(TestShards testShards) {
            this.ongoingRecoveries.triggerSplit(testShards.parentShard.shardId());
        }

        public void addReplicaRecoveriesBeforePrimariesInSync(TestShards testShards, TestShardSplitParams testShardSplitParams) {
            Map<ShardId, IndexShard> childPrimaries = new HashMap<>();
            Map<ShardId, IndexShardRoutingTable> childShardsRoutings = new HashMap<>();
            testShards.childContexts.forEach(context -> childPrimaries.put(context.getIndexShard().shardId(), context.getIndexShard()));
            AtomicInteger pendingRecoveries = new AtomicInteger();
            testShards.childReplicas.values().forEach(replicas -> replicas.forEach(replica -> pendingRecoveries.incrementAndGet()));
            for (List<IndexShard> replicas : testShards.childReplicas.values()) {
                for (IndexShard replica : replicas) {
                    addReplicaRecovery(
                        testShards.parentShard.shardId(),
                        ActionListener.wrap(r -> {
                            IndexShardRoutingTable routingTable = childShardsRoutings.getOrDefault(replica.shardId(),
                                new IndexShardRoutingTable.Builder(replica.shardId())
                                    .addShard(childPrimaries.get(replica.shardId()).routingEntry())
                                    .build());
                            routingTable = new IndexShardRoutingTable.Builder(routingTable)
                                .addShard(replica.routingEntry())
                                .build();
                            childShardsRoutings.put(replica.shardId(), routingTable);
                            try {
                                this.ongoingRecoveries.testShardUtils.recoverReplica(childPrimaries.get(replica.shardId()),
                                    replica, true, routingTable, testShards.parentShard);
                                if (pendingRecoveries.decrementAndGet() == 0) {
                                    startChildShards(testShards.parentShard.shardId());
                                }
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }, testShardSplitParams.recoveryException::set)
                    );
                }
            }
        }
    }

    static class TestClusterService extends ClusterService {

        private final Set<ClusterStateListener> testClusterStateListeners;

        public TestClusterService(Settings settings, ClusterSettings clusterSettings,
                                  ClusterManagerService clusterManagerService,
                                  ClusterApplierService clusterApplierService) {
            super(settings, clusterSettings, clusterManagerService, clusterApplierService);
            testClusterStateListeners = new HashSet<>();
        }


        public void removeListener(ClusterStateListener listener) {
            testClusterStateListeners.remove(listener);
            super.removeListener(listener);
        }

        public void addListener(ClusterStateListener listener) {
            testClusterStateListeners.add(listener);
        }
    }

    static class TestShardUtils extends IndexShardTestCase {
        private final List<IndexShard> replicatingShards = new ArrayList<>();
        private TestShards testShards;

        public void setTestShards(TestShards testShards) {
            this.testShards = testShards;
        }

        public void recoverReplica(IndexShard primary, IndexShard replica, boolean startShard, IndexShardRoutingTable routingTable,
                                   IndexShard parentShard) throws IOException {
            super.recoverReplicaInSync(replica, primary, startShard, routingTable, parentShard);
        }

        public IndexShard createReplica(IndexMetadata indexMetadata, MapperService mapperService, ShardRouting shardRouting) throws IOException {

            return super.newShard(shardRouting, indexMetadata, null,
                new InternalEngineFactory(), () -> {}, RetentionLeaseSyncer.EMPTY, null, mapperService);
        }

        public IndexShard newStartedShard() throws IOException {
            return super.newStartedShard(true);
        }

        private IndexShard createShard(IndexMetadata metadata, ShardRouting shardRouting, MapperService mapperService) throws IOException {
            return super.newShard(shardRouting, metadata, null, new InternalEngineFactory(),
                () -> {}, RetentionLeaseSyncer.EMPTY, null, mapperService);
        }

        private IndexShard createShard(boolean primary, RecoverySource recoverySource, String index,
                                       int shardId, IndexMetadata metadata, MapperService mapperService) throws IOException {
            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId(index, "_na_", shardId),
                randomAlphaOfLength(10),
                primary,
                ShardRoutingState.INITIALIZING,
                recoverySource
            );

            return super.newShard(shardRouting, metadata, null, new InternalEngineFactory(),
                () -> {}, RetentionLeaseSyncer.EMPTY, null, mapperService);
        }

        private IndexMetadata.Builder createMetadataBuilder(int replicaCount, int numberOfShards, String index) throws IOException {
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS,  numberOfShards)
                .build();

            return IndexMetadata.builder(index)
                .settings(indexSettings)
                .primaryTerm(0, primaryTerm)
                .putMapping("{ \"properties\": {} }");
        }

        private IndexShard createSplittingParent(int replicaCount, int numberOfShards, int numberOfChildren,
                                                 DiscoveryNode parentNode) throws IOException {
            int parentShardId = 0;
            SplitShardsMetadata.Builder splitShardsMetadataBuilder = new SplitShardsMetadata.Builder(numberOfShards);
            splitShardsMetadataBuilder.splitShard(parentShardId, numberOfChildren);
            String index = "test-index";
            IndexMetadata.Builder indexMetadataBuilder = createMetadataBuilder(replicaCount, numberOfShards, index);
            indexMetadataBuilder.splitShardsMetadata(splitShardsMetadataBuilder.build());
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                indexMetadata.getSettings(),
                "index"
            );
            IndexShard parentShard = createShard(true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                "test-index", 0, indexMetadata, mapperService);
            super.newStartedShard(p -> parentShard, true);
            IndexMetadata sourceShardMetadata = parentShard.indexSettings().getIndexMetadata();

            ShardRange[] childShardRanges = sourceShardMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId);
            ShardRouting parentRouting = parentShard.routingEntry().createRecoveringChildShards(childShardRanges, replicaCount);
            assignChildShards(parentRouting, parentNode);

            updateRoutingEntry(parentShard, parentRouting);
            return parentShard;
        }

        private void assignChildShards(ShardRouting parentRouting, DiscoveryNode parentNode) {
            Map<ShardRouting, String> nodeMappings = new HashMap<>();
            for (ShardRouting childShard : parentRouting.getRecoveringChildShards()) {
                if (childShard.primary() == true) {
                    nodeMappings.put(childShard, parentNode.getId());
                } else {
                    nodeMappings.put(childShard, randomAlphaOfLength(10));
                }
            }
            parentRouting.assignChildShards(nodeMappings);
        }

        public void addReplicatingShards(List<IndexShard> newReplicatingShards) {
            replicatingShards.addAll(newReplicatingShards);
        }

        public void indexDocument(IndexShard indexShard, IndexRequest indexRequest) throws IOException, InterruptedException {

            CountDownLatch latch = new CountDownLatch(1);
            SetOnce<Exception> ex = new SetOnce<>();
            BulkItemRequest[] items = new BulkItemRequest[1];
            for (int i = 0; i < items.length; i++) {
                items[i] = new BulkItemRequest(i, indexRequest);
            }
            BulkShardRequest bulkShardRequest = new BulkShardRequest(indexShard.shardId(), WriteRequest.RefreshPolicy.NONE, items);
            SetOnce<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>> res = new SetOnce<>();

            TransportShardBulkAction.performOnPrimary(
                bulkShardRequest,
                indexShard,
                null,
                threadPool::absoluteTimeInMillis,
                (update, shardId, listener) -> updateMappingsOnShard(update, shardId, listener, indexShard),
                listener -> listener.onResponse(null),
                ActionListener.wrap(r -> {
                    indexShard.sync();
                    updateCheckpoints(indexShard, indexShard);
                    for (IndexShard replica : replicatingShards) {
                        Translog.Location location = TransportShardBulkAction.performOnReplica(bulkShardRequest, replica);
                        assertNotNull(location);
                        long primaryGlobalCkp = indexShard.getLastKnownGlobalCheckpoint();
                        replica.sync();
                        replica.updateGlobalCheckpointOnReplica(primaryGlobalCkp, "operation");
                        updateCheckpoints(indexShard, replica);
                    }
                    res.set(r);
                    latch.countDown();
                }, e -> {
                    ex.set(e);
                    latch.countDown();
                }),
                threadPool,
                ThreadPool.Names.WRITE
            );
            latch.await();
            String exTrace = ex.get() != null ? exceptionTrace(ex.get()) : null;
            assertNull("Indexing failed with exception " + exTrace, ex.get());
            assertNotNull(res.get());
            assertNull(res.get().finalResponseIfSuccessful.getResponses()[0].getFailure());
        }

        private void updateCheckpoints(IndexShard primary, IndexShard indexShard) {
            primary.updateLocalCheckpointForShard(indexShard.routingEntry().allocationId().getId(), indexShard.getLocalCheckpoint());
            primary.updateGlobalCheckpointForShard(indexShard.routingEntry().allocationId().getId(), indexShard.getLastSyncedGlobalCheckpoint());
        }

        private void updateMappingsOnShard(Mapping update, ShardId shardId, ActionListener<Void> listener, IndexShard indexShard) {
            try {
                IndexMetadata indexMetadata = IndexMetadata.builder(indexShard.indexSettings().getIndexMetadata())
                    .putMapping(update.toString())
                    .build();
                super.updateMappings(indexShard, indexMetadata);
            } catch (Exception mapEx) {
                listener.onFailure(mapEx);
            }
            listener.onResponse(null);
        }

        public void closeShards(TestShards shards) throws IOException {
            closeShards(shards.parentShard);
            for (List<IndexShard> childReplicas : shards.childReplicas.values()) {
                closeShards(childReplicas);
            }
            closeShards(shards.replicas);
            for (InPlaceShardRecoveryContext context : shards.childContexts) {
                closeShards(context.getIndexShard());
            }
        }

        private String exceptionTrace(Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        }

        public Set<String> getShardDocIds(IndexShard indexShard) throws IOException {
            return getShardDocUIDs(indexShard);
        }
    }

    static class TestIndexer {
        Thread[] t;
        AtomicInteger startIdx;
        IndexShard indexShard;
        TestShardUtils testShardUtils;
        CountDownLatch latch;
        Semaphore maxDocs;

        public TestIndexer(int threads, IndexShard indexShard, TestShardUtils testShardUtils, int maxDocs, int start) {
            this.t = new Thread[threads];
            this.maxDocs = new Semaphore(maxDocs);
            for (int i = 0;i < threads ;i++) {
                t[i] = createIndexerThread();
            }
            this.startIdx = new AtomicInteger(start);
            this.indexShard = indexShard;
            this.testShardUtils = testShardUtils;
            this.latch = new CountDownLatch(threads);
        }

        private Thread createIndexerThread() {
            return new Thread(() -> {
                try {
                    String index = indexShard.shardId().getIndex().getName();
                    AtomicInteger maxMappings = new AtomicInteger(randomIntBetween(100,500));
                    while (maxDocs.tryAcquire(250, TimeUnit.MILLISECONDS)) {
                        try {
                            IndexRequest indexRequest = new IndexRequest();
                            indexRequest.index(index).id(Integer.toString(startIdx.getAndIncrement()))
                                .source("count", randomInt())
                                .source("point", randomFloat())
                                .source("description", randomUnicodeOfCodepointLength(100))
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                            if (maxMappings.getAndDecrement() > 0 && randomBoolean()) {
                                indexRequest.source(randomAlphaOfLength(10), randomUnicodeOfCodepointLength(100));
                            }
                            testShardUtils.indexDocument(indexShard, indexRequest);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to index docs", ex);
                } finally {
                    latch.countDown();
                }
            });
        }

        public void start() {
            for (Thread thread : t) {
                thread.start();
            }
        }

        public void waitForAllDocs() throws InterruptedException {
            latch.await();
        }

        public static void verifyDocs(List<IndexShard> shards, int docsIndexed, TestShardUtils testShardUtils) throws IOException {
            Set<String> docIds = new HashSet<>();
            for (IndexShard shard : shards) {
                Set<String> shardDocs = testShardUtils.getShardDocIds(shard);
                for (String docId : shardDocs) {
                    assertFalse(docIds.contains(docId));
                    docIds.add(docId);
                }
            }

            for (int i=0; i < docsIndexed; i++) {
                if (!docIds.contains(String.valueOf(i))) {
                    System.out.println("Doc not found with id " + i);
                }
                assertTrue(docIds.contains(String.valueOf(i)));
            }
        }
    }

    private static void indexDocs(IndexShard shard, TestShardUtils testShardUtils, int numDocs, TestShardSplitParams testShardSplitParams) throws InterruptedException, IOException {
        int previouslyIndexed = testShardSplitParams.docsIndexed;
        TestIndexer testIndexer = new TestIndexer(4,  shard, testShardUtils, numDocs, testShardSplitParams.docsIndexed);
        testIndexer.start();
        testIndexer.waitForAllDocs();
        testShardSplitParams.incrementDocsIndexed(testIndexer.startIdx.get() - previouslyIndexed);
        TestIndexer.verifyDocs(List.of(shard), testShardSplitParams.docsIndexed, testShardUtils);
    }

    private TestShards createShards(int numberOfChildren, int replicaCount,
                                    int numDocs, TestShardSplitParams testShardSplitParams)
        throws InterruptedException, IOException {

        TestShardUtils testShardUtils = testShardSplitParams.testSplitResources.testShardUtils;
        DiscoveryNode localNode = testShardSplitParams.testSplitResources.localNode;
        IndexShard parentShard = testShardUtils.createSplittingParent(replicaCount, 1, numberOfChildren, localNode);
        List<IndexShard> replicas = new ArrayList<>();
        IndexShardRoutingTable routingTable = new IndexShardRoutingTable.Builder(parentShard.shardId())
            .addShard(parentShard.routingEntry())
            .build();
        for (int i = 0; i < replicaCount; i++) {
            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                parentShard.shardId(),
                randomAlphaOfLength(10),
                false,
                ShardRoutingState.INITIALIZING,
                RecoverySource.PeerRecoverySource.INSTANCE
            );
            IndexShard replica = testShardUtils.createReplica(parentShard.indexSettings().getIndexMetadata(),
                parentShard.mapperService(), shardRouting);
            routingTable = new IndexShardRoutingTable.Builder(routingTable)
                .addShard(shardRouting)
                .build();
            testShardUtils.recoverReplica(parentShard, replica, true, routingTable, null);
            replicas.add(replica);
        }
        testShardUtils.addReplicatingShards(replicas);
        indexDocs(parentShard, testShardUtils, numDocs, testShardSplitParams);

        List<InPlaceShardRecoveryContext> contexts = new ArrayList<>(numberOfChildren);
        Map<ShardId, List<IndexShard>> childReplicas = new HashMap<>();
        for (ShardRouting childShardRouting : parentShard.routingEntry().getRecoveringChildShards()) {
            if (childShardRouting.primary()) {
                RecoveryState recoveryState = new RecoveryState(childShardRouting, localNode, localNode);
                IndexShard childShard = testShardUtils.createShard(parentShard.indexSettings().getIndexMetadata(),
                    childShardRouting, parentShard.mapperService());
                childShard.markAsRecovering("from in-place shard split", recoveryState);
                contexts.add(new InPlaceShardRecoveryContext(recoveryState, childShard, parentShard));
            } else {
                childReplicas.putIfAbsent(childShardRouting.shardId(), new ArrayList<>());
                IndexShard childReplica = testShardUtils.createReplica(parentShard.indexSettings().getIndexMetadata(),
                    parentShard.mapperService(), childShardRouting);
                childReplicas.get(childShardRouting.shardId()).add(childReplica);
            }
        }

        TestShards testShards = new TestShards(parentShard, replicas, contexts, childReplicas);
        testShardSplitParams.testSplitResources.testShardUtils.setTestShards(testShards);
        return testShards;
    }

    private void addAndStartRecovery(TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService,
                                     TestShards shards,
                                     ActionListener<Void> recoveryListener,
                                     TestShardSplitParams testShardSplitParams) {

        List<ShardRouting> childShardRoutings = new ArrayList<>();
        List<InPlaceShardRecoveryContext> contexts = shards.childContexts;
        for (InPlaceShardRecoveryContext childContext : contexts) {
            childShardRoutings.add(childContext.getIndexShard().routingEntry());
        }
        IndexShard parentShard = shards.parentShard;

        IndicesClusterStateService indicesClusterStateService = mock(IndicesClusterStateService.class);
        doAnswer(invocation -> {
            recoveryListener.onResponse(null);
            return null;
        }).when(indicesClusterStateService).handleChildRecoveriesDone(any(ShardRouting.class), anyLong(), any(RecoverySource.InPlaceShardSplitRecoverySource.class));
        doAnswer(invocation -> {
            Exception ex = invocation.getArgument(2, Exception.class);
            recoveryListener.onFailure(ex);
            return null;
        }).when(indicesClusterStateService).handleChildRecoveriesFailure(any(), anyBoolean(), any());
        InPlaceShardSplitRecoveryListener listener = new InPlaceShardSplitRecoveryListener(
            childShardRoutings, indicesClusterStateService, parentShard.routingEntry(), parentShard.getOperationPrimaryTerm()
        );

        DiscoveryNode localNode = testShardSplitParams.testSplitResources.localNode;
        StartRecoveryRequest request = new StartRecoveryRequest(
            parentShard.shardId(),
            "N/A",
            localNode,
            localNode,
            Store.MetadataSnapshot.EMPTY,
            false,
            -1,
            SequenceNumbers.UNASSIGNED_SEQ_NO
        );

        inPlaceShardSplitRecoveryService.addAndStartRecovery(contexts, localNode, parentShard, listener, request, parentShard.indexSettings().getIndexMetadata());
    }

    private void initiateSplit(TestShards shards, TestShardSplitParams testShardSplitParams,
                               TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService) throws IOException {

        TestShardUtils testShardUtils = testShardSplitParams.testSplitResources.testShardUtils;
        ActionListener<Void> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                testShardSplitParams.recoverySuccessful.set(true);
                List<IndexShard> childShards = new ArrayList<>();
                shards.childContexts.forEach(context -> {
                    assertEquals(IndexShardState.POST_RECOVERY, context.getIndexShard().state());
                    childShards.add(context.getIndexShard());
                });

                assertFalse(inPlaceShardSplitRecoveryService.ongoingRecoveries.isRecoveryOngoing(shards.parentShard.shardId()));
                try {
                    TestIndexer.verifyDocs(childShards, testShardSplitParams.docsIndexed, testShardUtils);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                assertFalse(inPlaceShardSplitRecoveryService.ongoingRecoveries.isRecoveryOngoing(shards.parentShard.shardId()));
                testShardSplitParams.recoveryException.set(e);
            }
        };

        addAndStartRecovery(inPlaceShardSplitRecoveryService, shards, responseListener,testShardSplitParams);
    }

    private void verifySplit(TestShardSplitParams testShardSplitParams, TestShardUtils testShardUtils) {
        String exTrace = testShardSplitParams.recoveryException.get() != null ?
            testShardUtils.exceptionTrace(testShardSplitParams.recoveryException.get()) : null;
        assertNull("Recovery failed with exception " + exTrace, testShardSplitParams.recoveryException.get());
        assertEquals(Boolean.TRUE, testShardSplitParams.recoverySuccessful.get());
    }

    private void verifyFailure(TestShardSplitParams testShardSplitParams) {
        assertNotNull("Expected exception thrown from split recovery", testShardSplitParams.recoveryException.get());
        assertNull(testShardSplitParams.recoverySuccessful.get());
    }

    static class TestShardSplitParams {
        final TestSplitResources testSplitResources;
        final boolean indexDocsForTranslogReplay;
        final boolean continueIndexingAfterReplay;
        final boolean initiateSplitImmediately;
        final boolean testRecoveryFailure;
        final boolean testParentShardFailure;
        final boolean testRecoveryCancelled;
        volatile int docsIndexed = 0;
        final SetOnce<Boolean> recoverySuccessful = new SetOnce<>();
        final SetOnce<Exception> recoveryException = new SetOnce<>();

        private TestShardSplitParams( TestSplitResources testSplitResources, boolean indexDocsForTranslogReplay,
                                    boolean continueIndexingAfterReplay,
                                    boolean initiateSplitImmediately, boolean testRecoveryFailure,
                                      boolean testParentShardFailure, boolean testRecoveryCancelled) {
            this.indexDocsForTranslogReplay = indexDocsForTranslogReplay;
            this.continueIndexingAfterReplay = continueIndexingAfterReplay;
            this.testSplitResources = testSplitResources;
            this.initiateSplitImmediately = initiateSplitImmediately;
            this.testRecoveryFailure = testRecoveryFailure;
            this.testParentShardFailure = testParentShardFailure;
            this.testRecoveryCancelled = testRecoveryCancelled;
        }

        public synchronized void incrementDocsIndexed(int docsIndexed) {
            this.docsIndexed += docsIndexed;
        }

        static class TestBuilder {
            TestSplitResources testSplitResources;
            boolean indexDocsForTranslogReplay = false;
            boolean continueIndexingAfterReplay = false;
            boolean initiateSplitImmediately = false;
            boolean testRecoveryFailure = false;
            boolean testParentShardFailure = false;
            boolean testRecoveryCancelled = false;

            public TestBuilder(TestSplitResources testSplitResources) {
                this.testSplitResources = testSplitResources;
            }

            public TestBuilder indexDocsForTranslogReplay() {
                indexDocsForTranslogReplay = true;
                return this;
            }

            public TestBuilder continueIndexingAfterReplay() {
                continueIndexingAfterReplay = true;
                return this;
            }

            public TestBuilder initiateSplitImmediately() {
                initiateSplitImmediately = true;
                return this;
            }

            public TestBuilder testRecoveryFailure() {
                testRecoveryFailure = true;
                return this;
            }

            public TestBuilder testParentShardFailure() {
                testParentShardFailure = true;
                return this;
            }

            public TestBuilder testRecoveryCancelled() {
                testRecoveryCancelled = true;
                return this;
            }

            TestShardSplitParams build() {
                return new TestShardSplitParams(testSplitResources, indexDocsForTranslogReplay,
                    continueIndexingAfterReplay, initiateSplitImmediately, testRecoveryFailure,
                    testParentShardFailure, testRecoveryCancelled);
            }
        }
    }

    static class TestShards {
        final IndexShard parentShard;
        final List<IndexShard> replicas;
        final List<InPlaceShardRecoveryContext> childContexts;
        final Map<ShardId, List<IndexShard>> childReplicas;

        public TestShards(IndexShard parentShard, List<IndexShard> replicas,
                          List<InPlaceShardRecoveryContext> childContexts, Map<ShardId, List<IndexShard>> childReplicas) {
            this.parentShard = parentShard;
            this.replicas = replicas;
            this.childContexts = childContexts;
            this.childReplicas = childReplicas;
        }
    }

    public void testSplitServiceAddedRemovedFromClusterListener() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        try {
            testSplitResources.setUp();
            InPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new InPlaceShardSplitRecoveryService(
                testSplitResources.indicesService,
                new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
            );

            inPlaceShardSplitRecoveryService.start();
            assertTrue(testSplitResources.clusterService.testClusterStateListeners.contains(inPlaceShardSplitRecoveryService));

            inPlaceShardSplitRecoveryService.stop();
            assertFalse(testSplitResources.clusterService.testClusterStateListeners.contains(inPlaceShardSplitRecoveryService));
        } finally {
            testSplitResources.tearDown();
        }
    }

    public void testShardSplit() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .initiateSplitImmediately().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(4, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testShardSplitEmptyCommit() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .initiateSplitImmediately().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = 0;
            shards = createShards(3, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testNonEmptyTranslogAndReplication() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().initiateSplitImmediately().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(3, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testEmptyCommitAndReplication() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().initiateSplitImmediately().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = 0;
            shards = createShards(3, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testWithIngestionAfterTranslogReplay() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().continueIndexingAfterReplay().initiateSplitImmediately().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(3, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testWithReplicas() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().continueIndexingAfterReplay().build();
            TestInPlaceShardSplitRecoveryService splitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            splitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(3, 3, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, splitRecoveryService);
            splitRecoveryService.addReplicaRecoveriesBeforePrimariesInSync(shards, testShardSplitParams);
            splitRecoveryService.triggerSplit(shards);
            verifySplit(testShardSplitParams, testSplitResources.testShardUtils);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testShardSplitRecoveryFailure() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().continueIndexingAfterReplay().initiateSplitImmediately().testRecoveryFailure().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(4, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifyFailure(testShardSplitParams);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testParentShardFailure() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().continueIndexingAfterReplay().initiateSplitImmediately()
                .testParentShardFailure().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(4, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifyFailure(testShardSplitParams);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

    public void testRecoveryCancelled() throws Exception {
        TestSplitResources testSplitResources = new TestSplitResources();
        TestShards shards = null;
        try {
            testSplitResources.setUp();
            RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
            TestShardSplitParams testShardSplitParams = new TestShardSplitParams.TestBuilder(testSplitResources)
                .indexDocsForTranslogReplay().continueIndexingAfterReplay().initiateSplitImmediately()
                .testRecoveryCancelled().build();
            TestInPlaceShardSplitRecoveryService inPlaceShardSplitRecoveryService = new TestInPlaceShardSplitRecoveryService(
                recoverySettings, testShardSplitParams);
            inPlaceShardSplitRecoveryService.start();

            int numDocs = scaledRandomIntBetween(2000, 5000);
            shards = createShards(4, 0, numDocs, testShardSplitParams);
            initiateSplit(shards, testShardSplitParams, inPlaceShardSplitRecoveryService);
            verifyFailure(testShardSplitParams);
        } finally {
            if (shards != null) {
                testSplitResources.testShardUtils.closeShards(shards);
            }
            testSplitResources.tearDown();
        }
    }

}
