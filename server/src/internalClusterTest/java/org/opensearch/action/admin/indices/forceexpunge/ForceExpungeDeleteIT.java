/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forceexpunge;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceExpungeDeletesShardRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceExpungeDeletesShardResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class ForceExpungeDeleteIT extends OpenSearchIntegTestCase {



    public void testForceMergeUUIDConsistent() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        final String index = "test-index";
        createIndex(
            index,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen(index);
        int numDocs = randomIntBetween(5000, 10000);
        Set<String> ids = null;
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, MapperService.SINGLE_MAPPING_NAME, client(), numDocs, 4)) {
            indexer.setIgnoreIndexingFailures(false);
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            ids = indexer.getIds();
        }

        int docsToDelete = numDocs/2;
        int deleted = 0;
        for (String id : ids) {
            if (deleted == docsToDelete) {
                break;
            }
            boolean refresh = deleted == docsToDelete - 1;
            DeleteResponse res = client().delete(new DeleteRequest().index(index).id(id)
                .setRefreshPolicy(refresh ? WriteRequest.RefreshPolicy.IMMEDIATE: WriteRequest.RefreshPolicy.NONE))
                .actionGet();
            assertEquals(RestStatus.OK, res.status());
            deleted++;
        }
        assertHitCount(client().prepareSearch(index).setSize(0).get(), numDocs - deleted);


        final ClusterState state = clusterService().state();
        final IndexRoutingTable indexShardRoutingTables = state.routingTable().getIndicesRouting().get(index);
        final IndexShardRoutingTable shardRouting = indexShardRoutingTables.getShards().get(0);
        final String primaryNodeId = shardRouting.primaryShard().currentNodeId();
        final String replicaNodeId = shardRouting.replicaShards().get(0).currentNodeId();
        final Index idx = shardRouting.primaryShard().index();
        final IndicesService primaryIndicesService = internalCluster().getInstance(
            IndicesService.class,
            state.nodes().get(primaryNodeId).getName()
        );
        final IndicesService replicaIndicesService = internalCluster().getInstance(
            IndicesService.class,
            state.nodes().get(replicaNodeId).getName()
        );
        final IndexShard primary = primaryIndicesService.indexService(idx).getShard(0);
        final IndexShard replica = replicaIndicesService.indexService(idx).getShard(0);

        assertThat(getForceMergeUUID(primary), nullValue());
        assertThat(getForceMergeUUID(replica), nullValue());

        ForceExpungeDeletesShardRequest expungeRequest = new ForceExpungeDeletesShardRequest(index, 0);
        final ForceExpungeDeletesShardResponse expungeResponse = client().admin().indices().forceExpungeDelete(expungeRequest).get();
        primary.flush(new FlushRequest().waitIfOngoing(true));
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.docs(true);
        IndicesStatsResponse response = client().execute(IndicesStatsAction.INSTANCE, request).actionGet();
        System.out.println(response);
//
//        assertEquals(0, expungeResponse.getShardId().id());
//        assertNotNull(expungeResponse.getForceMergeUUID());
//
//        // Force flush to force a new commit that contains the force flush UUID
//        final FlushResponse flushResponse = client().admin().indices().prepareFlush(index).setForce(true).get();
//        assertThat(flushResponse.getFailedShards(), is(0));
//        assertThat(flushResponse.getSuccessfulShards(), is(2));
//
//        final String primaryForceMergeUUID = getForceMergeUUID(primary);
//        assertThat(primaryForceMergeUUID, notNullValue());
//
//        final String replicaForceMergeUUID = getForceMergeUUID(replica);
//        assertThat(replicaForceMergeUUID, notNullValue());
//        assertThat(primaryForceMergeUUID, is(replicaForceMergeUUID));
    }

    private static String getForceMergeUUID(IndexShard indexShard) throws IOException {
        try (GatedCloseable<IndexCommit> wrappedIndexCommit = indexShard.acquireLastIndexCommit(true)) {
            return wrappedIndexCommit.get().getUserData().get(Engine.FORCE_MERGE_UUID_KEY);
        }
    }
}
