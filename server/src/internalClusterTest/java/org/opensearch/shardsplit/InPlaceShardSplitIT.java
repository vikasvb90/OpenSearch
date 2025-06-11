/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.shardsplit;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitRequest;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.indices.recovery.inplacesplit.InPlaceShardSplitRecoveryService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.search.SearchHits;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class InPlaceShardSplitIT extends OpenSearchIntegTestCase {
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);
    private Set<Integer> triggerSplitAndGetChildShardIds(int parentShardId, int numberOfSplits) {

        InPlaceShardSplitRequest request = new InPlaceShardSplitRequest("test", parentShardId, numberOfSplits);
        InPlaceShardSplitResponse response = client().admin().indices().inPlaceShardSplit(request).actionGet();
        assertAcked(response);
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        ShardRange[] shards = indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId);
        return Arrays.stream(shards).map(ShardRange::getShardId).collect(Collectors.toSet());
    }

    private void waitForSplit(int numberOfSplits, Set<Integer> childShardIds, Set<Integer> parentShardIds, int replicaCount) throws Exception {
        final long maxWaitTimeMs = Math.max(190 * 1000, 200 * numberOfSplits);

        assertBusy(() -> {
            ShardStats[] shardStats = client().admin().indices().prepareStats("test").get().getShards();
            int startedChildShards = 0, startedChildReplicas = 0;
            for (ShardStats shardStat : shardStats) {
                ShardRouting shardRouting = shardStat.getShardRouting();
                if (shardRouting.primary() && parentShardIds.contains(shardRouting.shardId().id()) && shardStat.getShardRouting().started()) {
                    throw new Exception("Splitting of shard id " + shardRouting.shardId().id() + " failed ");
                } else if (childShardIds.contains(shardStat.getShardRouting().shardId().id())) {
                    startedChildShards++;
                    if (shardRouting.primary() == false) {
                        startedChildReplicas++;
                    }
                }

            }
            assertEquals(numberOfSplits * (replicaCount + 1) * parentShardIds.size(), startedChildShards);
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);

        assertClusterHealth();
        logger.info("Shard split completed");
    }

    private void waitForShardsStarted() throws Exception {
        final long maxWaitTimeMs = Math.max(190 * 1000, 200);
        assertBusy(() -> {
            ShardStats[] shardStats = client().admin().indices().prepareStats("test").get().getShards();
            for (ShardStats shardStat : shardStats) {
                ShardRouting shardRouting = shardStat.getShardRouting();
                assertEquals(ShardRoutingState.STARTED, shardRouting.state());
            }
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);

        ensureGreen("test");
    }

    private void assertClusterHealth() {
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse, notNullValue());
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
        assertThat(clusterHealthResponse.status(), equalTo(RestStatus.OK));
        assertThat(clusterHealthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    private void verifyAfterSplit(long totalIndexedDocs, Set<String> ids, Set<Integer> parentShardIds, Set<Integer> childShardIds) throws InterruptedException {
        ClusterState clusterState = internalCluster().clusterManagerClient().admin().cluster().prepareState().get().getState();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        if (!childShardIds.isEmpty()) {
            Set<Integer> newServingChildShardIds = new HashSet<>();
            for (Integer parentShardId : parentShardIds) {
                assertNotNull(indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId));
                ShardRange[] shards = indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId);
                Set<Integer> currentShardIds = Arrays.stream(shards).map(ShardRange::getShardId).collect(Collectors.toSet());
                for (int shardId : currentShardIds) {
                    assertFalse(parentShardIds.contains(shardId));
                    if (childShardIds.contains(shardId)) {
                        newServingChildShardIds.add(shardId);
                    }
                }
            }
            assertEquals(childShardIds, newServingChildShardIds);
        }

        refresh("test");
        SearchHits hits = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize((int) totalIndexedDocs)
            .seqNoAndPrimaryTerm(true)
            .setPreference("_primary")
            .storedFields()
            .execute()
            .actionGet()
            .getHits();

        assertThat(hits.getTotalHits().value, equalTo(totalIndexedDocs));
        for (String id : ids) {
            // Make sure there is no duplicate doc.
            assertHitCount(client().prepareSearch("test").setSize(0)
                .setQuery(matchQuery("_id", id)).setPreference("_primary").get(), 1);
        }
        ensureGreen("test");
        logger.info("Shard is split successfully");
    }

    public void testShardSplit() throws Exception {
        internalCluster().startNodes(2);
        int replicaCount = 2;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 3)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            numDocs = scaledRandomIntBetween(5000, 7500);
            logger.info("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
        }
    }

    public void testConcurrentShardSplitsOnIndex() throws Exception {
        internalCluster().startNodes(2);
        int replicaCount = 2;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 3)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            numDocs = scaledRandomIntBetween(5000, 7500);
            logger.info("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardIdA = 0, parentShardIdB = 1;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardIdA, numberOfSplits);
            childShardIds.addAll(triggerSplitAndGetChildShardIds(parentShardIdB, numberOfSplits));
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardIdA, parentShardIdB), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardIdA, parentShardIdB), childShardIds);
        }
    }

    public void testShardSplitFailedAsParentFailed() throws Exception {
        internalCluster().startNodes(3);
        int replicaCount = 2;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 3)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        ClusterStateResponse clusterStateResp = client().admin().cluster().prepareState().get();
        ShardRouting parentRouting = clusterStateResp.getState().getRoutingNodes().shards(shard -> shard.id() == 0 && shard.primary()).get(0);
        String parentNode = clusterStateResp.getState().nodes().get(parentRouting.currentNodeId()).getName();
        IndexShard parentShard = getIndexShard(parentNode, parentRouting.shardId(), "test");
        int numDocs = scaledRandomIntBetween(200, 500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            numDocs = scaledRandomIntBetween(5000, 7500);
            logger.info("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            parentShard.failShard("Failing to test split failure", new Exception("Failing parent primary from test"));
            waitForShardsStarted();
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
        }
    }

    public void testShardSplitFailedAsChildFailed() throws Exception {
        internalCluster().startNodes(3);
        int replicaCount = 0, numberOfShards = 3;
        String indexName = "test";
        prepareCreate(indexName, Settings.builder().put("index.number_of_shards", numberOfShards)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        ClusterStateResponse clusterStateResp = client().admin().cluster().prepareState().get();
        ShardRouting parentRouting = clusterStateResp.getState().getRoutingNodes().shards(shard -> shard.id() == 0 && shard.primary()).get(0);
        String parentNode = clusterStateResp.getState().nodes().get(parentRouting.currentNodeId()).getName();
        int numDocs = scaledRandomIntBetween(200, 500);
        int numberOfSplits = 3, parentShardId = 0;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            numDocs = scaledRandomIntBetween(1000, 4000 - numDocs);
            logger.info("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);

            ShardId childShardId = new ShardId(parentRouting.index(), numberOfShards);
            triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);

            InPlaceShardSplitRecoveryService splitRecoveryService = internalCluster().getInstance(InPlaceShardSplitRecoveryService.class, parentNode);
            assertBusy(() -> assertTrue(splitRecoveryService.isRecoveryInProgress(parentRouting.shardId())), 2, TimeUnit.MINUTES);

            indexer.stopAndAwaitStopped();

            IndicesClusterStateService indicesClusterStateService = internalCluster().getInstance(IndicesClusterStateService.class, parentNode);
            indicesClusterStateService.handleRecoveryFailure(parentRouting, true, new Exception("testing-failing-parent"));

            getClusterState().metadata().index(indexName);
            assertBusy(() -> assertTrue(getClusterState().metadata().index(indexName) == null ||
                !getClusterState().metadata().index(indexName).getSplitShardsMetadata()
                    .isSplitOfShardInProgress(parentRouting.shardId().id())), 2, TimeUnit.MINUTES);

            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
        }

    }

    private Optional<ShardRouting> getShardRouting(ShardId shardId, ClusterState state) {
        return Optional.ofNullable(state.routingTable())
            .filter(indexRoutingTables -> indexRoutingTables.hasIndex(shardId.getIndex()))
            .map(rt -> rt.index(shardId.getIndex()))
            .map(rt -> rt.shard(shardId.getId()))
            .map(IndexShardRoutingTable::shards)
            .map(routing -> routing.get(0));
    }


    public void testSplittingShardHavingNonEmptyCommit() throws Exception {
        internalCluster().startNodes(2);
        int replicaCount = 0;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);

            flushAndRefresh("test");
            long testDocs = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(numDocs + 100)
                .seqNoAndPrimaryTerm(true)
                .setPreference("_local")
                .storedFields()
                .execute()
                .actionGet()
                .getHits().getTotalHits().value;
            assertEquals(numDocs, testDocs);

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.debug("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
        }
    }

    public void testSplittingShardWithNoTranslogReplay() throws Exception {
        internalCluster().startNodes(2);
        int replicaCount = 0;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            indexer.stopAndAwaitStopped();
            flushAndRefresh("test");
            long testDocs = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(numDocs + 100)
                .seqNoAndPrimaryTerm(true)
                .setPreference("_local")
                .storedFields()
                .execute()
                .actionGet()
                .getHits().getTotalHits().value;
            assertEquals(numDocs, testDocs);

            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
        }
    }

    public void testSearchRedirection() throws Exception {
        internalCluster().startNodes(3);
        int replicaCount = randomIntBetween(0, 2);
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        TestThreadPool testThreadPool = new TestThreadPool(InPlaceShardSplitIT.class.getName() + randomAlphaOfLength(5));
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            indexer.stopAndAwaitStopped();
            flushAndRefresh("test");
            long testDocs = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(numDocs + 100)
                .seqNoAndPrimaryTerm(true)
                .setPreference("_local")
                .storedFields()
                .execute()
                .actionGet()
                .getHits().getTotalHits().value;
            assertEquals(numDocs, testDocs);

            AtomicBoolean stopped = new AtomicBoolean(false);
            Runnable r = () -> {
                while (stopped.get() == false) {
                    Exception testEx = null;
                    try {
                        client().prepareSearch("test")
                            .setQuery(matchAllQuery())
                            .setSize((int) testDocs)
                            .seqNoAndPrimaryTerm(true)
                            .setPreference("_local")
                            .storedFields()
                            .execute()
                            .actionGet()
                            .getHits();
                    } catch (Exception ex) {
                        if (ex instanceof NodeNotConnectedException == false && ex instanceof NodeClosedException == false) {
                            testEx = ex;
                        }
                    }
                    assertNull(testEx);
                }
            };
            testThreadPool.executor("generic").execute(r);

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.debug("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
            stopped.set(true);
        } finally {
            ThreadPool.terminate(testThreadPool, 5, TimeUnit.SECONDS);
        }
    }

    public void testSingleShardSearchRedirection() throws Exception {
        internalCluster().startNodes(3);
        int replicaCount = 2;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        TestThreadPool testThreadPool = new TestThreadPool(InPlaceShardSplitIT.class.getName() + randomAlphaOfLength(5));
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            indexer.stopAndAwaitStopped();
            flushAndRefresh("test");
            long testDocs = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(numDocs + 100)
                .seqNoAndPrimaryTerm(true)
                .setPreference("_local")
                .storedFields()
                .execute()
                .actionGet()
                .getHits().getTotalHits().value;
            assertEquals(numDocs, testDocs);

            AtomicBoolean stopped = new AtomicBoolean(false);
            final int maxGetDoc = numDocs - 1;
            Runnable r = () -> {
                while (stopped.get() == false) {
                    Exception testEx = null;
                    try {
                        GetResponse hits = client().prepareGet().setIndex("test")
                            .setId(randomIntBetween(1, maxGetDoc) + "")
                            .execute()
                            .actionGet();
                    } catch (Exception ex) {
                        if (ex instanceof NodeNotConnectedException == false && ex instanceof NodeClosedException == false) {
                            testEx = ex;
                        }
                    }
                    assertNull(testEx);
                }
            };
            testThreadPool.executor("generic").execute(r);

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.debug("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
            Thread.sleep(5000);
            stopped.set(true);
        } finally {
            ThreadPool.terminate(testThreadPool, 5, TimeUnit.SECONDS);
        }
    }

    public void testMultiGetSearchRedirection() throws Exception {
        internalCluster().startNodes(3);
        int replicaCount = 0;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        TestThreadPool testThreadPool = new TestThreadPool(InPlaceShardSplitIT.class.getName() + randomAlphaOfLength(5));
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);
            indexer.stopAndAwaitStopped();
            flushAndRefresh("test");
            long testDocs = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(numDocs + 100)
                .seqNoAndPrimaryTerm(true)
                .setPreference("_local")
                .storedFields()
                .execute()
                .actionGet()
                .getHits().getTotalHits().value;
            assertEquals(numDocs, testDocs);

            AtomicBoolean stopped = new AtomicBoolean(false);
            final int maxGetDoc = numDocs - 1;
            Runnable r = () -> {
                while (stopped.get() == false) {
                    Exception testEx = null;
                    try {
                        MultiGetResponse hits = client().prepareMultiGet()
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .add("test", randomIntBetween(1, maxGetDoc) + "")
                            .get(TimeValue.MAX_VALUE);
                        for (MultiGetItemResponse response : hits.getResponses()) {
                            assertNull(response.getFailure());
                        }
                    } catch (Exception ex) {
                        if (ex instanceof NodeNotConnectedException == false && ex instanceof NodeClosedException == false) {
                            testEx = ex;
                        }
                    }
                    assertNull(testEx);
                }
            };
            testThreadPool.executor("generic").execute(r);

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.debug("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, Set.of(parentShardId), replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), Set.of(parentShardId), childShardIds);
            Thread.sleep(5000);
            stopped.set(true);
        } finally {
            ThreadPool.terminate(testThreadPool, 5, TimeUnit.SECONDS);
        }
    }

}
