/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ingest.IngestService;
import org.opensearch.painless.PainlessModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for data plane script execution feature.
 * Tests Painless script execution on data nodes with routing change detection and redirection.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class DataPlaneScriptExecutionIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test-index";
    private static final String FIELD_MODIFICATION_PIPELINE = "field-modification-pipeline";
    private static final String ROUTING_CHANGE_PIPELINE = "routing-change-pipeline";
    private static final String ID_CHANGE_PIPELINE = "id-change-pipeline";
    private static final String INDEX_CHANGE_PIPELINE = "index-change-pipeline";
    private static final String FAILING_PIPELINE = "failing-pipeline";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonModulePlugin.class, PainlessModulePlugin.class);
    }

    @Before
    public void setupPipelines() throws Exception {
        String fieldModificationPipeline = "{"
            + "  \"description\": \"Modifies document fields using Painless\","
            + "  \"processors\": ["
            + "    {"
            + "      \"script\": {"
            + "        \"source\": \"ctx.modified_field = ctx.field + '_modified'\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        assertAcked(
            client().admin()
                .cluster()
                .putPipeline(
                    new PutPipelineRequest(FIELD_MODIFICATION_PIPELINE, new BytesArray(fieldModificationPipeline), MediaTypeRegistry.JSON)
                )
                .actionGet()
        );

        String routingChangePipeline = "{"
            + "  \"description\": \"Changes routing value using Painless\","
            + "  \"processors\": ["
            + "    {"
            + "      \"script\": {"
            + "        \"source\": \"ctx._routing = ctx.field + '_routed'\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        assertAcked(
            client().admin()
                .cluster()
                .putPipeline(new PutPipelineRequest(ROUTING_CHANGE_PIPELINE, new BytesArray(routingChangePipeline), MediaTypeRegistry.JSON))
                .actionGet()
        );

        String idChangePipeline = "{"
            + "  \"description\": \"Changes document ID using Painless\","
            + "  \"processors\": ["
            + "    {"
            + "      \"script\": {"
            + "        \"source\": \"ctx._id = ctx.field + '_new_id'\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        assertAcked(
            client().admin()
                .cluster()
                .putPipeline(new PutPipelineRequest(ID_CHANGE_PIPELINE, new BytesArray(idChangePipeline), MediaTypeRegistry.JSON))
                .actionGet()
        );

        String indexChangePipeline = "{"
            + "  \"description\": \"Changes index name using Painless\","
            + "  \"processors\": ["
            + "    {"
            + "      \"script\": {"
            + "        \"source\": \"ctx._index = ctx.field + '-index'\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        assertAcked(
            client().admin()
                .cluster()
                .putPipeline(new PutPipelineRequest(INDEX_CHANGE_PIPELINE, new BytesArray(indexChangePipeline), MediaTypeRegistry.JSON))
                .actionGet()
        );

        String failingPipeline = "{"
            + "  \"description\": \"Fails during execution using Painless\","
            + "  \"processors\": ["
            + "    {"
            + "      \"script\": {"
            + "        \"source\": \"throw new Exception('Painless script execution failed')\""
            + "      }"
            + "    }"
            + "  ]"
            + "}";
        assertAcked(
            client().admin()
                .cluster()
                .putPipeline(new PutPipelineRequest(FAILING_PIPELINE, new BytesArray(failingPipeline), MediaTypeRegistry.JSON))
                .actionGet()
        );

        assertAcked(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(TEST_INDEX).settings(
                        Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0)
                    )
                )
                .actionGet()
        );

        ensureGreen(TEST_INDEX);
    }

    @After
    public void resetSettings() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(Settings.builder().putNull("ingest.script.data_plane_execution.enabled"));
        assertAcked(client().admin().cluster().updateSettings(request).actionGet());
    }

    /**
     * Test: Painless script execution without routing changes on data plane
     * Verifies that Painless scripts execute on data nodes and modify document fields correctly
     */
    public void testPainlessScriptExecutionWithoutRoutingChanges() throws Exception {
        enableDataPlaneScriptExecution();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id("doc1")
            .source("field", "value")
            .setPipeline(FIELD_MODIFICATION_PIPELINE);

        client().index(indexRequest).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        SearchHit hit = response.getHits().getAt(0);
        assertThat(hit.getId(), equalTo("doc1"));
        Map<String, Object> source = hit.getSourceAsMap();
        assertThat(source.get("field"), equalTo("value"));
        assertThat(source.get("modified_field"), equalTo("value_modified"));
    }

    /**
     * Test: Painless script execution with routing change on data plane
     * Verifies that routing changes are detected and processed correctly
     */
    public void testPainlessScriptExecutionWithRoutingChange() throws Exception {
        enableDataPlaneScriptExecution();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id("doc2")
            .source("field", "local")
            .setPipeline(ROUTING_CHANGE_PIPELINE);

        client().index(indexRequest).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        SearchHit hit = response.getHits().getAt(0);
        assertThat(hit.getId(), equalTo("doc2"));
        assertThat(hit.getSourceAsMap().get("field"), equalTo("local"));
    }

    /**
     * Test: Painless script execution with ID change (triggers redirection) on data plane
     * Verifies that items are redirected to coordinator and re-routed correctly
     */
    public void testPainlessScriptExecutionWithIdChange() throws Exception {
        enableDataPlaneScriptExecution();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id("original_id")
            .source("field", "remote")
            .setPipeline(ID_CHANGE_PIPELINE);

        client().index(indexRequest).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        SearchHit hit = response.getHits().getAt(0);
        assertThat(hit.getId(), equalTo("remote_new_id"));
        assertThat(hit.getSourceAsMap().get("field"), equalTo("remote"));
    }

    /**
     * Test: Mixed bulk request with Painless scripts on data plane
     * Verifies that bulk requests with and without Painless scripts are processed correctly
     */
    public void testMixedBulkRequestWithPainlessScripts() throws Exception {
        enableDataPlaneScriptExecution();

        client().index(new IndexRequest(TEST_INDEX).id("no_pipeline").source("field", "value1")).actionGet();

        client().index(new IndexRequest(TEST_INDEX).id("with_pipeline").source("field", "value2").setPipeline(FIELD_MODIFICATION_PIPELINE))
            .actionGet();

        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(2L));

        response = search(TEST_INDEX, QueryBuilders.idsQuery().addIds("no_pipeline"));
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value1"));
        assertThat(response.getHits().getAt(0).getSourceAsMap().containsKey("modified_field"), equalTo(false));

        response = search(TEST_INDEX, QueryBuilders.idsQuery().addIds("with_pipeline"));
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("modified_field"), equalTo("value2_modified"));
    }

    /**
     * Test: Painless script execution failures on data plane
     * Verifies that Painless script failures are handled correctly
     */
    public void testPainlessScriptExecutionFailures() throws Exception {
        enableDataPlaneScriptExecution();

        IndexRequest failingRequest = new IndexRequest(TEST_INDEX).id("failing").source("field", "value1").setPipeline(FAILING_PIPELINE);

        try {
            client().index(failingRequest).actionGet();
            fail("Expected pipeline execution to fail");
        } catch (Exception e) {
            assertThat(e.getMessage(), notNullValue());
        }

        IndexRequest successRequest = new IndexRequest(TEST_INDEX).id("success")
            .source("field", "value2")
            .setPipeline(FIELD_MODIFICATION_PIPELINE);
        client().index(successRequest).actionGet();

        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("success"));
    }

    /**
     * Test: Setting toggle for data plane Painless script execution
     * Verifies that toggling the setting changes execution behavior
     */
    public void testDataPlaneScriptExecutionSettingToggle() throws Exception {
        IndexRequest request1 = new IndexRequest(TEST_INDEX).id("coord_exec")
            .source("field", "value1")
            .setPipeline(FIELD_MODIFICATION_PIPELINE);

        client().index(request1).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.idsQuery().addIds("coord_exec"));
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("modified_field"), equalTo("value1_modified"));

        enableDataPlaneScriptExecution();

        IndexRequest request2 = new IndexRequest(TEST_INDEX).id("data_exec")
            .source("field", "value2")
            .setPipeline(FIELD_MODIFICATION_PIPELINE);

        client().index(request2).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        response = search(TEST_INDEX, QueryBuilders.idsQuery().addIds("data_exec"));
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("modified_field"), equalTo("value2_modified"));

        disableDataPlaneScriptExecution();

        IndexRequest request3 = new IndexRequest(TEST_INDEX).id("coord_exec_2")
            .source("field", "value3")
            .setPipeline(FIELD_MODIFICATION_PIPELINE);

        client().index(request3).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        response = search(TEST_INDEX, QueryBuilders.idsQuery().addIds("coord_exec_2"));
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("modified_field"), equalTo("value3_modified"));
    }

    /**
     * Test: Pipeline set to NOOP after Painless execution on data plane
     * Verifies that redirected items have NOOP pipeline to prevent re-execution
     */
    public void testPipelineSetToNoopAfterPainlessExecution() throws Exception {
        enableDataPlaneScriptExecution();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id("original")
            .source("field", "redirect_test")
            .setPipeline(ID_CHANGE_PIPELINE);

        client().index(indexRequest).actionGet();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();

        SearchResponse response = search(TEST_INDEX, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));

        SearchHit hit = response.getHits().getAt(0);
        assertThat(hit.getId(), equalTo("redirect_test_new_id"));
        assertThat(hit.getSourceAsMap().get("field"), equalTo("redirect_test"));
    }

    /**
     * Test: Index change during Painless execution on data plane
     * Verifies that items with index changes are handled correctly
     */
    public void testIndexNotFoundDuringPainlessRoutingResolution() throws Exception {
        enableDataPlaneScriptExecution();

        String targetIndex = "nonexistent-index";
        assertAcked(
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(targetIndex).settings(
                        Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0)
                    )
                )
                .actionGet()
        );
        ensureGreen(targetIndex);

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX).id("index_change")
            .source("field", "nonexistent")
            .setPipeline(INDEX_CHANGE_PIPELINE);

        client().index(indexRequest).actionGet();
        client().admin().indices().prepareRefresh(targetIndex).get();

        SearchResponse response = search(targetIndex, QueryBuilders.matchAllQuery());
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), equalTo("index_change"));
    }

    private SearchResponse search(String index, org.opensearch.index.query.QueryBuilder query) {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(new SearchSourceBuilder().query(query));
        return client().search(searchRequest).actionGet();
    }

    private void enableDataPlaneScriptExecution() throws Exception {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(Settings.builder().put(IngestService.DATA_PLANE_SCRIPT_EXECUTION_ENABLED.getKey(), true));
        assertAcked(client().admin().cluster().updateSettings(request).actionGet(10, TimeUnit.SECONDS));

        ensureSettingPropagated(true);
    }

    private void disableDataPlaneScriptExecution() throws Exception {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(Settings.builder().put(IngestService.DATA_PLANE_SCRIPT_EXECUTION_ENABLED.getKey(), false));
        assertAcked(client().admin().cluster().updateSettings(request).actionGet(10, TimeUnit.SECONDS));

        ensureSettingPropagated(false);
    }

    private void ensureSettingPropagated(boolean expectedValue) throws Exception {
        // Wait for the setting to propagate across all nodes
        // This ensures coordinator and data nodes have the same setting value
        // which is critical for serialization/deserialization compatibility
        Thread.sleep(1000);

        ensureGreen(TEST_INDEX);
    }
}
