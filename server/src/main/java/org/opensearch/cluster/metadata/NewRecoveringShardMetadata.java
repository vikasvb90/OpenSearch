/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Metadata of new recovering shards
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NewRecoveringShardMetadata extends AbstractDiffable<NewRecoveringShardMetadata> implements Writeable, ToXContentFragment {
    private static final String KEY_SOURCE_SHARD_ID = "source_shard_id";
    private static final String KEY_PRIMARY_TERM = "primary_term";
    private static final String KEY_IN_SYNC_ALLOCATION_IDS = "in_sync_allocation_ids";

    private final int shardId;
    private final int sourceShardId;
    final Set<String> inSyncAllocationIds;
    private long primaryTerm;

    public int getShardId() {
        return shardId;
    }

    public int getSourceShardId() {
        return sourceShardId;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewRecoveringShardMetadata that = (NewRecoveringShardMetadata) o;

        return shardId == that.shardId &&
            sourceShardId == that.sourceShardId &&
            primaryTerm == that.primaryTerm &&
            Objects.equals(inSyncAllocationIds, that.inSyncAllocationIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, sourceShardId, inSyncAllocationIds, primaryTerm);
    }

    private NewRecoveringShardMetadata(int shardId, int sourceShardId, Set<String> inSyncAllocationIds, long primaryTerm) {
        this.shardId = shardId;
        this.sourceShardId = sourceShardId;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.primaryTerm = primaryTerm;
    }

    public NewRecoveringShardMetadata(StreamInput in) throws IOException {
        this.shardId = in.readVInt();
        this.sourceShardId = in.readVInt();
        this.primaryTerm = in.readVLong();
        int key = in.readVInt();
        this.inSyncAllocationIds = DiffableUtils.StringSetValueSerializer.getInstance().read(in, key);;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeVInt(sourceShardId);
        out.writeVLong(primaryTerm);
        out.writeVInt(inSyncAllocationIds.size());
        DiffableUtils.StringSetValueSerializer.getInstance().write(inSyncAllocationIds, out);
    }

    public static Diff<NewRecoveringShardMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(NewRecoveringShardMetadata::new, in);
    }

    /**
     * Builder of new recovering shard metadata.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class Builder {
        private final int shardId;
        private int sourceShardId;
        private Set<String> inSyncAllocationIds;
        private long primaryTerm;

        public Builder(int shardId) {
            this.shardId = shardId;
        }

        public Builder sourceShardId(int sourceShardId) {
            this.sourceShardId = sourceShardId;
            return this;
        }

        public Builder inSyncAllocationIds(Set<String> inSyncAllocationIds) {
            this.inSyncAllocationIds = inSyncAllocationIds;
            return this;
        }

        public Builder primaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
            return this;
        }

        public NewRecoveringShardMetadata build() {
            return new NewRecoveringShardMetadata(shardId, sourceShardId, inSyncAllocationIds, primaryTerm);
        }
    }

    public static NewRecoveringShardMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        Builder builder = new Builder(Integer.parseInt(parser.currentName()));

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_IN_SYNC_ALLOCATION_IDS.equals(currentFieldName)) {
                    Set<String> allocationIds = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            allocationIds.add(parser.text());
                        }
                    }
                    builder.inSyncAllocationIds(allocationIds);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (KEY_PRIMARY_TERM.equals(currentFieldName)) {
                    builder.primaryTerm(parser.longValue());
                } else if (KEY_SOURCE_SHARD_ID.equals(currentFieldName)) {
                    builder.sourceShardId(parser.intValue());
                }
            }
        }

        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Integer.toString(getShardId()));
        builder.field(KEY_SOURCE_SHARD_ID, getSourceShardId());
        builder.startArray(KEY_IN_SYNC_ALLOCATION_IDS);
        for (String allocationId : getInSyncAllocationIds()) {
            builder.value(allocationId);
        }
        builder.endArray();

        builder.field(KEY_PRIMARY_TERM, getPrimaryTerm());
        builder.endObject();

        return builder;
    }


    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
