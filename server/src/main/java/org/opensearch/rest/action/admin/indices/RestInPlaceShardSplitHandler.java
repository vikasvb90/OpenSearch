/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitAction;
import org.opensearch.action.admin.indices.split.InPlaceShardSplitRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.tasks.LoggingTaskListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.support.master.AcknowledgedRequest.DEFAULT_TASK_EXECUTION_TIMEOUT;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestInPlaceShardSplitHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestInPlaceShardSplitHandler.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    @Override
    public String getName() {
        return "in_place_shard_split_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(POST, "/{index}/_shard/{shard_id}/_split"), new Route(PUT, "/{index}/_shard/{shard_id}/_split"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        InPlaceShardSplitRequest inPlaceShardSplitRequest = new InPlaceShardSplitRequest(
            request.param("index"),
            request.paramAsInt("shard_id", -1),
            request.paramAsInt("number_of_splits", -1)
        );
        request.applyContentParser(inPlaceShardSplitRequest::fromXContent);
        inPlaceShardSplitRequest.timeout(request.paramAsTime("timeout", inPlaceShardSplitRequest.timeout()));
        inPlaceShardSplitRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", inPlaceShardSplitRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(inPlaceShardSplitRequest, request, deprecationLogger, getName());

        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.admin().indices().inPlaceShardSplit(inPlaceShardSplitRequest, new RestToXContentListener<>(channel));
        } else {
            // running resizing index asynchronously, return a task immediately and store the task's result when it completes
            inPlaceShardSplitRequest.setShouldStoreResult(true);
            inPlaceShardSplitRequest.timeout(request.paramAsTime("task_execution_timeout", DEFAULT_TASK_EXECUTION_TIMEOUT));
            /*
             * Add some validation before so the user can get some error immediately. The
             * task can't totally validate until it starts but this is better than nothing.
             */
            ActionRequestValidationException validationException = inPlaceShardSplitRequest.validate();
            if (validationException != null) {
                throw validationException;
            }
            return sendTask(
                client.getLocalNodeId(),
                client.executeLocally(InPlaceShardSplitAction.INSTANCE, inPlaceShardSplitRequest, LoggingTaskListener.instance())
            );
        }
    }
}
