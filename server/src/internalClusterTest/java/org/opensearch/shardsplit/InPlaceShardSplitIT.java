/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.shardsplit;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitRequest;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.cat.RestClusterManagerAction;
import org.opensearch.search.SearchHits;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
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

    private void waitForSplit(int numberOfSplits, Set<Integer> childShardIds, int parentShardId, int replicaCount) throws Exception {
        final long maxWaitTimeMs = Math.max(190 * 1000, 200 * numberOfSplits);

        assertBusy(() -> {
            ShardStats[] shardStats = client().admin().indices().prepareStats("test").get().getShards();
            int startedChildShards = 0;
            for (ShardStats shardStat : shardStats) {
                ShardRouting shardRouting = shardStat.getShardRouting();
                if (shardRouting.primary() && shardRouting.shardId().id() == parentShardId && shardStat.getShardRouting().started()) {
                    throw new Exception("Splitting of shard id " + parentShardId + " failed ");
                } else if (childShardIds.contains(shardStat.getShardRouting().shardId().id())) {
                    startedChildShards++;
                }
            }
            if (numberOfSplits + (replicaCount + 1) == startedChildShards) {
                System.out.println();
            }
            assertEquals(numberOfSplits * (replicaCount + 1), startedChildShards);
//            ClusterState state = client().admin().cluster().prepareState().get().getState();
//            int startedChildReplicas = 0;
//            for (RoutingNode routingNode : state.getRoutingNodes()) {
//                for (ShardRouting shardRouting : routingNode) {
//                    if (shardRouting.isStartedChildReplica()) {
//                        startedChildReplicas++;
//                    }
//                }
//            }
//            assertEquals(numberOfSplits * (replicaCount), startedChildReplicas);
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);

        assertClusterHealth();
        logger.info("Shard split completed");
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

    private void verifyAfterSplit(long totalIndexedDocs, Set<String> ids, int parentShardId, Set<Integer> childShardIds) throws InterruptedException {
        ClusterState clusterState = internalCluster().clusterManagerClient().admin().cluster().prepareState().get().getState();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        assertNotNull(indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId));
        ShardRange[] shards = indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId);
        Set<Integer> currentShardIds = Arrays.stream(shards).map(ShardRange::getShardId).collect(Collectors.toSet());
        assertEquals(childShardIds, currentShardIds);
        Set<Integer> newServingChildShardIds = new HashSet<>();
        for (int shardId : currentShardIds) {
            assertTrue(parentShardId != shardId);
            if (childShardIds.contains(shardId)) newServingChildShardIds.add(shardId);
        }
        assertEquals(childShardIds, newServingChildShardIds);

        refresh("test");
//        ShardStats[] stats = client().admin().indices().prepareStats("test").get().getShards();
//        for (ShardStats shardStat : stats) {
//            logger.info("Shard stat after first indexing of shard " + shardStat.getShardRouting().shardId().id() + " docs: "
//                + shardStat.getStats().indexing.getTotal().getIndexCount() + " seq no: " + shardStat.getSeqNoStats().getMaxSeqNo());
//        }

        SearchHits hits = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize((int) totalIndexedDocs)
            .storedFields()
            .execute()
            .actionGet()
            .getHits();
        assertThat(hits.getTotalHits().value, equalTo(totalIndexedDocs));
        for (String id : ids) {
            // Make sure there is no duplicate doc.
            assertHitCount(client().prepareSearch("test").setSize(0)
                .setQuery(matchQuery("_id", id)).get(), 1);
        }
        logger.info("Shard is split successfully");
    }

    public void testShardSplit() throws Exception {
        internalCluster().startNodes(2);
        int replicaCount = 2;
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 3)
            .put("index.number_of_replicas", replicaCount)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(1500, 2400);
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
            waitForSplit(numberOfSplits, childShardIds, parentShardId, replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), parentShardId, childShardIds);
        }
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
            ShardStats shardStat = client().admin().indices().prepareStats("test").get().getShards()[0];
            assertEquals(numDocs, shardStat.getCommitStats().getNumDocs());

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.debug("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, parentShardId, replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            indexer.pauseIndexing();
            indexer.stopAndAwaitStopped();
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), parentShardId, childShardIds);
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
            ShardStats shardStat = client().admin().indices().prepareStats("test").get().getShards()[0];
            assertEquals(numDocs, shardStat.getCommitStats().getNumDocs());

            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, parentShardId, replicaCount);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            verifyAfterSplit(indexer.totalIndexedDocs(), indexer.getIds(), parentShardId, childShardIds);
        }
    }

}
