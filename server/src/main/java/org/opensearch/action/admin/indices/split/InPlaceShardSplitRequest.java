/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Request class to split a local shard of an index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InPlaceShardSplitRequest extends AcknowledgedRequest<InPlaceShardSplitRequest> implements ToXContentObject {
    public static final ObjectParser<InPlaceShardSplitRequest, Void> PARSER = new ObjectParser<>("in_place_shard_split_request");

    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareInt(constructorArg(), new ParseField("shard_id"));
        PARSER.declareInt(constructorArg(), new ParseField("number_of_splits"));
    }

    private final String index;
    private final int shardId;
    private final int numberOfSplits;
    private boolean shouldStoreResult;

    public InPlaceShardSplitRequest(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        shardId = in.readInt();
        numberOfSplits = in.readInt();
    }

    public InPlaceShardSplitRequest(String index, int shardId, int numberOfSplits) {
        this.index = index;
        this.shardId = shardId;
        this.numberOfSplits = numberOfSplits;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeInt(shardId);
        out.writeInt(numberOfSplits);
    }

    /**
     * Should this task store its result after it has finished?
     */
    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public int getSplitInto() {
        return numberOfSplits;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("index", index);
            builder.field("shard_id", shardId);
            builder.field("number_of_splits", numberOfSplits);
        }
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        PARSER.parse(parser, this, null);
    }

    @Override
    public String toString() {
        return "In place shard split of index [" + index + "] shard id [" + shardId + "] into [" + numberOfSplits + "] shards";
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", null);
        }
        if (shardId < 0) {
            validationException = addValidationError("invalid shard id", validationException);
        }
        if (numberOfSplits < 2) {
            validationException = addValidationError("invalid split configuration.", validationException);
        }

        return validationException;
    }

    @Override
    public String getDescription() {
        return this.toString();
    }
}
