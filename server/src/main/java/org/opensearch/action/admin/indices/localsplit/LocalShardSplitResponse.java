/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.localsplit;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Split response of online split of a local shard
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LocalShardSplitResponse extends AcknowledgedResponse {

    private static final ConstructingObjectParser<LocalShardSplitResponse, Void> PARSER = new ConstructingObjectParser<>(
        "local_shard_split_response",
        true,
        args -> new LocalShardSplitResponse((boolean) args[0], (String) args[1], (int) args[2], (int) args[3])
    );

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareInt(constructorArg(), new ParseField("shard_id"));
        PARSER.declareInt(constructorArg(), new ParseField("split_into"));
    }

    private final String index;
    private final int shardId;
    private final int splitInto;

    LocalShardSplitResponse(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        shardId = in.readInt();
        splitInto = in.readInt();
    }

    public LocalShardSplitResponse(final boolean acknowledged, final String index, final int shardId, final int splitInto) {
        super(acknowledged);
        this.index = index;
        this.shardId = shardId;
        this.splitInto = splitInto;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeInt(shardId);
        out.writeInt(splitInto);
    }

    public static LocalShardSplitResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            LocalShardSplitResponse that = (LocalShardSplitResponse) o;
            return shardId == that.shardId && splitInto == that.splitInto && Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index, shardId, splitInto);
    }
}
