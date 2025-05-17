/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

@ExperimentalApi
public class ForceExpungeDeletesShardResponse extends ActionResponse implements ToXContentObject  {
    private final String FIELD_SHARD = "shard";
    private final String FIELD_FORCE_EXPUNGE_UUID = "force_expunge_uuid";
    private final ShardId shardId;
    private final String forceMergeUUID;

    public  ForceExpungeDeletesShardResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        forceMergeUUID = in.readString();
    }

    public ForceExpungeDeletesShardResponse(ShardId shardId, String forceMergeUUID) {
       this.shardId = shardId;
       this.forceMergeUUID = forceMergeUUID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(forceMergeUUID);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_SHARD, shardId.toXContent(builder, params));
        builder.field(FIELD_FORCE_EXPUNGE_UUID, forceMergeUUID);
        builder.endObject();
        return builder;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public String getForceMergeUUID() {
        return forceMergeUUID;
    }
}
