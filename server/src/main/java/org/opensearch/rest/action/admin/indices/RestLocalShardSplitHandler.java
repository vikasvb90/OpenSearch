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
import org.opensearch.action.admin.indices.localsplit.LocalShardSplit;
import org.opensearch.action.admin.indices.localsplit.LocalShardSplitAction;
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

public class RestLocalShardSplitHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestLocalShardSplitHandler.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    @Override
    public String getName() {
        return "local_shard_split_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(POST, "/{index}/_local_split/{shard_id}"), new Route(PUT, "/{index}/_local_split/{shard_id}"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        LocalShardSplit localShardSplit = new LocalShardSplit(
            request.param("index"),
            request.paramAsInt("shard_id", -1),
            request.paramAsInt("split_into", -1)
        );
        request.applyContentParser(localShardSplit::fromXContent);
        localShardSplit.timeout(request.paramAsTime("timeout", localShardSplit.timeout()));
        localShardSplit.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", localShardSplit.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(localShardSplit, request, deprecationLogger, getName());

        if (request.paramAsBoolean("wait_for_completion", true)) {
            return channel -> client.admin().indices().localShardSplit(localShardSplit, new RestToXContentListener<>(channel));
        } else {
            // running resizing index asynchronously, return a task immediately and store the task's result when it completes
            localShardSplit.setShouldStoreResult(true);
            localShardSplit.timeout(request.paramAsTime("task_execution_timeout", DEFAULT_TASK_EXECUTION_TIMEOUT));
            /*
             * Add some validation before so the user can get some error immediately. The
             * task can't totally validate until it starts but this is better than nothing.
             */
            ActionRequestValidationException validationException = localShardSplit.validate();
            if (validationException != null) {
                throw validationException;
            }
            return sendTask(
                client.getLocalNodeId(),
                client.executeLocally(LocalShardSplitAction.INSTANCE, localShardSplit, LoggingTaskListener.instance())
            );
        }
    }
}
