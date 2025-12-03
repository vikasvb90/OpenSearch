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

package org.opensearch.action.bulk;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.support.WriteResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Transport response for a bulk shard request
 *
 * @opensearch.internal
 */
public class BulkShardResponse extends ReplicationResponse implements WriteResponse {

    private final ShardId shardId;
    private final BulkItemResponse[] responses;
    private final List<BulkItemRequest> redirectedItems;
    private final boolean serializeRedirectedItems;

    /**
     * Constructor for deserialization with flag to control reading redirected items.
     * This flag should be set based on the cluster setting for data plane script execution.
     *
     * @param in StreamInput to read from
     * @param readRedirectedItems Whether to attempt reading redirected items from the stream
     */
    BulkShardResponse(StreamInput in, boolean readRedirectedItems) throws IOException {
        super(in);
        shardId = new ShardId(in);
        responses = in.readArray(i -> new BulkItemResponse(shardId, i), BulkItemResponse[]::new);
        
        // Always read the boolean marker for wire protocol compatibility
        boolean hasRedirectedItems = in.readBoolean();
        
        // Read redirected items data only if marker is true
        if (hasRedirectedItems) {
            int size = in.readVInt();
            if (readRedirectedItems) {
                // Setting is enabled, read and store the redirected items
                List<BulkItemRequest> items = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    items.add(new BulkItemRequest(shardId, in));
                }
                this.redirectedItems = Collections.unmodifiableList(items);
            } else {
                // Setting is disabled, read and discard the redirected items to stay in sync
                for (int i = 0; i < size; i++) {
                    new BulkItemRequest(shardId, in); // Read and discard
                }
                this.redirectedItems = Collections.emptyList();
            }
        } else {
            this.redirectedItems = Collections.emptyList();
        }
        this.serializeRedirectedItems = readRedirectedItems;
    }

    // NOTE: public for testing only
    public BulkShardResponse(ShardId shardId, BulkItemResponse[] responses) {
        this(shardId, responses, Collections.emptyList(), false);
    }

    /**
     * Constructor with redirected items for data plane script execution
     *
     * @param shardId The shard ID
     * @param responses Array of bulk item responses for successfully processed items
     * @param redirectedItems List of items that need to be redirected to the coordinator for re-routing
     * @param serializeRedirectedItems Whether to serialize redirected items (true if request had pipelines)
     */
    public BulkShardResponse(
        ShardId shardId,
        BulkItemResponse[] responses,
        List<BulkItemRequest> redirectedItems,
        boolean serializeRedirectedItems
    ) {
        this.shardId = shardId;
        this.responses = responses;
        this.redirectedItems = redirectedItems != null ? Collections.unmodifiableList(new ArrayList<>(redirectedItems))
            : Collections.emptyList();
        this.serializeRedirectedItems = serializeRedirectedItems;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public BulkItemResponse[] getResponses() {
        return responses;
    }

    /**
     * @return The list of items that need to be redirected to the coordinator for re-routing.
     *         Used when painless scripts execute on data nodes and modify routing keys.
     */
    public List<BulkItemRequest> getRedirectedItems() {
        return redirectedItems;
    }

    /**
     * @return true if there are items that need to be redirected, false otherwise
     */
    public boolean hasRedirectedItems() {
        return redirectedItems != null && !redirectedItems.isEmpty();
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        /*
         * Each DocWriteResponse already has a location for whether or not it forced a refresh so we just set that information on the
         * response.
         */
        for (BulkItemResponse response : responses) {
            DocWriteResponse r = response.getResponse();
            if (r != null) {
                r.setForcedRefresh(forcedRefresh);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeArray((o, item) -> item.writeThin(out), responses);
        
        // Always write the boolean marker for wire protocol compatibility
        // When setting is disabled, always write false
        // When setting is enabled, write true/false based on whether there are redirected items
        if (serializeRedirectedItems && !redirectedItems.isEmpty()) {
            out.writeBoolean(true);
            out.writeVInt(redirectedItems.size());
            for (BulkItemRequest item : redirectedItems) {
                item.writeThin(out);
            }
        } else {
            out.writeBoolean(false);
        }
    }
}
