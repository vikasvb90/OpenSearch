/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.UUID;

@ExperimentalApi
public class ForceExpungeDeletesShardRequest extends SingleShardRequest<ForceExpungeDeletesShardRequest> {
    private final int shardId;
    private final String expungeDeletesUUID;

    /**
     * Constructs a new request against the specified index.
     */
    public ForceExpungeDeletesShardRequest(String index, int shardId) {
        super(index);
        this.shardId = shardId;
        this.expungeDeletesUUID = UUID.randomUUID().toString();
    }

    public ForceExpungeDeletesShardRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = in.readVInt();
        this.expungeDeletesUUID = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
        out.writeString(expungeDeletesUUID);
    }

    public String getExpungeDeletesUUID() {
        return expungeDeletesUUID;
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }

    public int getShardId() {
        return shardId;
    }
}
