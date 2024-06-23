/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.shardsplit;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitRequest;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.SearchHits;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
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
        return new HashSet<>(indexMetadata.getChildShardIds(parentShardId));
    }

    private void waitForSplit(int numberOfSplits, Set<Integer> childShardIds, int parentShardId) throws Exception {
        final long maxWaitTimeMs = Math.max(190 * 1000, 200 * numberOfSplits);

        assertBusy(() -> {
            ShardStats[] shardStats = client().admin().indices().prepareStats("test").get().getShards();
            int startedChildShards = 0;
            for (ShardStats shardStat : shardStats) {
                if (shardStat.getShardRouting().shardId().id()  == parentShardId && shardStat.getShardRouting().started()) {
                    throw new Exception("Splitting of shard id " + parentShardId + " failed ");
                } else if (childShardIds.contains(shardStat.getShardRouting().shardId().id())) {
                    startedChildShards ++;
                }
            }
            assertEquals(numberOfSplits, startedChildShards);
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);

        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
        System.out.println("Shard split completed");
    }

    private void verifyAfterSplit(BackgroundIndexer indexer, int parentShardId, Set<Integer> childShardIds) throws InterruptedException {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        IndexMetadata indexMetadata = clusterState.metadata().index("test");
        assertTrue(indexMetadata.isParentShard(parentShardId));
        assertEquals(childShardIds, new HashSet<>(indexMetadata.getChildShardIds(parentShardId)));
        Set<Integer> newServingChildShardIds = new HashSet<>();
        for (int shardId : indexMetadata.getServingShardIds()) {
            assertTrue(parentShardId != shardId);
            if (childShardIds.contains(shardId)) newServingChildShardIds.add(shardId);
        }
        assertEquals(childShardIds, newServingChildShardIds);

        indexer.pauseIndexing();
        indexer.stopAndAwaitStopped();
        refresh("test");
        SearchHits hits = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setSize((int) indexer.totalIndexedDocs())
            .storedFields()
            .execute()
            .actionGet()
            .getHits();
//        assertThat(hits.getTotalHits().value, equalTo(indexer.totalIndexedDocs()));
        List<String> ids = indexer.getOrderedIds();
        int idx=-1;
        for (String id : ids) {
            idx++;
            logger.info("Ordered id " + idx);
            // Make sure there is no duplicate doc.
            assertHitCount(client().prepareSearch("test").setSize(0)
                .setQuery(matchQuery("_id", id)).get(), 1);
        }
        logger.info("Shard is split successfully");
    }

    public void testShardSplit() throws Exception {
        internalCluster().startNodes(2);
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);

            numDocs = scaledRandomIntBetween(200, 1000);
            logger.info("--> Allow indexer to index [{}] more documents", numDocs);
            indexer.continueIndexing(numDocs);
            int numberOfSplits = 3, parentShardId = 0;
            logger.info("--> starting split...");
            Set<Integer> childShardIds = triggerSplitAndGetChildShardIds(parentShardId, numberOfSplits);
            logger.info("--> waiting for shards to be split ...");
            waitForSplit(numberOfSplits, childShardIds, parentShardId);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            verifyAfterSplit(indexer, parentShardId, childShardIds);
        }
    }

    public void testSplittingShardHavingNonEmptyCommit() throws Exception {
        internalCluster().startNodes(2);
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)).get();
        ensureGreen();
        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs)) {
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
            waitForSplit(numberOfSplits, childShardIds, parentShardId);
            logger.info("--> Shard split completed ...");
            logger.info("--> Verifying after split ...");
            verifyAfterSplit(indexer, parentShardId, childShardIds);
        }
    }

}
