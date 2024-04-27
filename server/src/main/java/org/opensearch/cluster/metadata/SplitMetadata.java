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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


public class SplitMetadata extends AbstractDiffable<SplitMetadata>  {
    private final int parentShardId;
    private final Set<Integer> childShardIds;

    private final int routingFactor;
    private final int routingNumShards;

    public SplitMetadata(int parentShardId, Set<Integer> childShardIds, int parentRoutingFactor) {
        this.parentShardId = parentShardId;
        this.childShardIds = childShardIds;
        int numChildShards = childShardIds.size();
        this.routingNumShards = calculateNumRoutingShards(parentRoutingFactor, numChildShards);
        this.routingFactor = this.routingNumShards / numChildShards;
    }

    public SplitMetadata(int parentShardId, Set<Integer> childShardIds, int routingFactor, int routingNumShards) {
        this.parentShardId = parentShardId;
        this.childShardIds = childShardIds;
        this.routingFactor = routingFactor;
        this.routingNumShards = routingNumShards;
    }

    /**
     * Calculate the number of routing shards for a given parent shard
     * @param parentRoutingFactor routing factor of parent shard
     * @param numOfChildShards number of child shards
     * @return the number of routing shards
     */
    public static int calculateNumRoutingShards(int parentRoutingFactor, int numOfChildShards) {
        if(numOfChildShards > parentRoutingFactor) {
            throw new IllegalArgumentException("Cannot split further");
        }
        int x = parentRoutingFactor / numOfChildShards;
        int log2OrfDivShards = 32 - Integer.numberOfLeadingZeros(x - 1);

        int numSplits = ((x & (x - 1)) == 0)? log2OrfDivShards: log2OrfDivShards -1;
        return numOfChildShards * (1 << numSplits);
    }

    public int getRoutingFactor() {
        return routingFactor;
    }

    public int getRoutingNumShards() {
        return routingNumShards;
    }

    public Set<Integer> getChildShards() {
        return childShardIds;
    }

    public int getParentShardId() {
        return parentShardId;
    }

    public SplitMetadata(StreamInput in) throws IOException {
        parentShardId = in.readVInt();
        childShardIds = in.readSet(StreamInput::readVInt);
        routingFactor = in.readVInt();
        routingNumShards = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(parentShardId);
        out.writeCollection(childShardIds, StreamOutput::writeVInt);
        out.writeVInt(routingFactor);
        out.writeVInt(routingNumShards);
    }

    public static Diff<SplitMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitMetadata::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitMetadata)) return false;
        SplitMetadata that = (SplitMetadata) o;

        return parentShardId == that.parentShardId &&
            routingFactor == that.routingFactor &&
            routingNumShards == that.routingNumShards &&
            Objects.equals(childShardIds, that.childShardIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentShardId, childShardIds, routingFactor, routingNumShards);
    }

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.startObject(String.valueOf(parentShardId));
        builder.startArray("child_shard_ids");
        for (final Integer childShard : childShardIds) {
            builder.value(childShard);
        }
        builder.endArray();
        builder.field("routing_factor", routingFactor);
        builder.field("routing_num_shards", routingNumShards);
        builder.endObject();
    }

    public static SplitMetadata parse(XContentParser parser, String currentFieldName) throws IOException {
        int parentShardId = Integer.parseInt(currentFieldName);
        Set<Integer> childShardIds = new HashSet<>();
        int routingFactor = 0;
        int routingShard = 0;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                switch (fieldName) {
                    case "child_shard_ids":
                        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                childShardIds.add(parser.intValue());
                            }
                        }
                        break;
                    case "routing_factor":
                        routingFactor = parser.intValue();
                        break;
                    case "routing_num_shards":
                        routingShard = parser.intValue();
                        break;
                }
            }
        }
        return new SplitMetadata(parentShardId, childShardIds, routingFactor, routingShard);
    }

}
