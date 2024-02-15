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
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SplitShardsMetadata extends AbstractDiffable<SplitShardsMetadata> implements ToXContentFragment {
    private static final int MINIMUM_RANGE_LENGTH_THRESHOLD = 1000;
    public static final int SPLIT_NOT_IN_PROGRESS = -2;

    private static final String KEY_ROOT_SHARDS_TO_ALL_CHILDREN = "root_shards_to_all_children";
    private static final String KEY_NUMBER_OF_ROOT_SHARDS = "num_of_root_shards";
    private static final String KEY_TEMP_SHARD_ID_TO_CHILD_SHARDS = "temp_shard_id_to_child_shards";
    private static final String KEY_MAX_SHARD_ID = "max_shard_id";
    private static final String KEY_IN_PROGRESS_SPLIT_SHARD_ID = "in_progress_split_shard_id";

    // Root shard id to flat list of all child shards under root.
    private final ShardRange[][] rootShardsToAllChildren;
    // Mapping of a parent shard ID to children.
    private final Map<Integer, ShardRange[]> parentToChildShards;
    private final int maxShardId;
    private final int inProgressSplitShardId;

    private SplitShardsMetadata(ShardRange[][] rootShardsToAllChildren, Map<Integer, ShardRange[]> parentToChildShards,
                                int inProgressSplitShardId, int maxShardId) {

        this.rootShardsToAllChildren = rootShardsToAllChildren;
        this.parentToChildShards = parentToChildShards;
        this.maxShardId = maxShardId;
        this.inProgressSplitShardId = inProgressSplitShardId;
    }

    public SplitShardsMetadata(StreamInput in) throws IOException {
        int numberOfRootShards = in.readVInt();
        this.rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
        for (int i=0; i < numberOfRootShards; i++) {
            this.rootShardsToAllChildren[i] = in.readOptionalArray(ShardRange::new, ShardRange[]::new);
        }
        this.maxShardId = in.readVInt();
        this.inProgressSplitShardId = in.readInt();
        this.parentToChildShards = in.readMap(StreamInput::readInt, i-> i.readArray(ShardRange::new, ShardRange[]::new));
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rootShardsToAllChildren.length);
        for (ShardRange[] rootShardsToAllChild : rootShardsToAllChildren) {
            out.writeOptionalArray(rootShardsToAllChild);
        }
        out.writeVInt(this.maxShardId);
        out.writeInt(this.inProgressSplitShardId);
        out.writeMap(this.parentToChildShards, StreamOutput::writeInt, StreamOutput::writeArray);
    }

    public int getShardIdOfHash(int rootShardId, int hash, boolean includeInProgressChildren) {
        // First check if we have child shards against this root shard.
        if (rootShardsToAllChildren[rootShardId] == null) {
            if (includeInProgressChildren && parentToChildShards.containsKey(rootShardId)) {
                ShardRange shardRange = binarySearchShards(parentToChildShards.get(rootShardId), hash);
                assert shardRange != null;
                return shardRange.getShardId();
            }
            return rootShardId;
        }

        ShardRange[] existingChildShards = rootShardsToAllChildren[rootShardId];
        ShardRange shardRange = binarySearchShards(existingChildShards, hash);
        assert shardRange != null;

        if (includeInProgressChildren && parentToChildShards.containsKey(shardRange.getShardId())) {
            shardRange = binarySearchShards(parentToChildShards.get(shardRange.getShardId()), hash);
        }
        assert shardRange != null;

        return shardRange.getShardId();
    }

    private ShardRange binarySearchShards(ShardRange[] childShards, int hash) {
        int low = 0, high = childShards.length - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            ShardRange midShard = childShards[mid];
            if (midShard.contains(hash)) {
                return midShard;
            } else if (hash < midShard.getStart()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    public int getNumberOfRootShards() {
        return rootShardsToAllChildren.length;
    }

    public int getNumberOfShards() {
        return maxShardId + 1;
    }

    public ShardRange[] getChildShardsOfParent(int shardId) {
        if (parentToChildShards.containsKey(shardId) == false) {
            return null;
        }

        ShardRange[] childShards = new ShardRange[parentToChildShards.get(shardId).length];
        int childShardIdx = 0;
        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShards[childShardIdx++] = childShard.copy();
        }
        return childShards;
    }

    public int numberOfEmptyParentShards() {
        int emptyParents = parentToChildShards.size();
        if (inProgressSplitShardId != SPLIT_NOT_IN_PROGRESS) {
            emptyParents -= 1;
        }
        return emptyParents;
    }

    private static void validateShardRanges(int shardId, ShardRange[] shardRanges) {
        Integer start = null;
        for (ShardRange shardRange : shardRanges) {
            validateBounds(shardRange, start);
            long rangeEnd = shardRange.getEnd();
            long rangeLength = rangeEnd - shardRange.getStart() + 1;
            if (rangeLength < MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException(
                    "Shard range from " + shardRange.getStart() + " to " + shardRange.getEnd()
                        + " is below shard range threshold of " + MINIMUM_RANGE_LENGTH_THRESHOLD);
            }

            start = shardRange.getEnd();
        }

        if (start == null) {
            throw new IllegalArgumentException("No shard range defined for child shards of shard " + shardId);
        }

        if (start != Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Shard range from " + (start + 1) + " to " + Integer.MAX_VALUE
                    + " is missing from the list of shard ranges");
        }
    }

    private static void validateBounds(ShardRange shardRange, Integer start) {
        if (start == null) {
            if (shardRange.getStart() != Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                    "Shard range from " + Integer.MIN_VALUE + " to " + (shardRange.getStart() - 1)
                        + " is missing from the list of shard ranges");
            }
        } else if (shardRange.getStart() != start + 1) {
            throw new IllegalArgumentException(
                "Shard range from " + (start + 1) + " to " + (shardRange.getStart() - 1)
                    + " is missing from the list of shard ranges");
        }
    }

    public static class Builder {
        private final ShardRange[][] rootShardsToAllChildren;
        private final Map<Integer, ShardRange[]> parentToChildShards;
        private int maxShardId;
        private int inProgressSplitShardId;

        public Builder(int numberOfShards) {
            maxShardId = numberOfShards - 1;
            rootShardsToAllChildren = new ShardRange[numberOfShards][];
            parentToChildShards = new HashMap<>();
            inProgressSplitShardId = SPLIT_NOT_IN_PROGRESS;
        }

        public Builder(SplitShardsMetadata splitShardsMetadata) {
            this.maxShardId = splitShardsMetadata.maxShardId;

            this.rootShardsToAllChildren = new ShardRange[splitShardsMetadata.rootShardsToAllChildren.length][];
            for (int i = 0; i < splitShardsMetadata.rootShardsToAllChildren.length; i++) {
                if (splitShardsMetadata.rootShardsToAllChildren[i] != null) {
                    this.rootShardsToAllChildren[i] = new ShardRange[splitShardsMetadata.rootShardsToAllChildren[i].length];
                    int j = 0;
                    for (ShardRange childShard : splitShardsMetadata.rootShardsToAllChildren[i]) {
                        this.rootShardsToAllChildren[i][j++] = childShard.copy();
                    }
                }
            }

            this.parentToChildShards = new HashMap<>();
            for (Integer parentShardId : splitShardsMetadata.parentToChildShards.keySet()) {
                // Getting a copy of child shards for this parent.
                ShardRange[] childShards = splitShardsMetadata.getChildShardsOfParent(parentShardId);
                this.parentToChildShards.put(parentShardId, childShards);
            }

            inProgressSplitShardId = splitShardsMetadata.inProgressSplitShardId;
        }

        /**
         * Create metadata of new child shards for the provided shard id.
         * @param splitShardId Shard id to split
         * @param numberOfChildren Number of child shards this shard is going to have.
         */
        public void splitShard(int splitShardId, int numberOfChildren) {
            if (inProgressSplitShardId != SPLIT_NOT_IN_PROGRESS || parentToChildShards.containsKey(splitShardId)) {
                throw new IllegalArgumentException("Split of shard [" + inProgressSplitShardId +
                    "] is already in progress or completed.");
            }

            inProgressSplitShardId = splitShardId;

            Tuple<Integer, ShardRange> shardTuple = findRootAndShard(splitShardId, rootShardsToAllChildren);
            if (shardTuple == null) {
                throw new IllegalArgumentException("Invalid shard id provided for splitting");
            }
            ShardRange parentShard = shardTuple.v2();

            long rangeSize = ((long)parentShard.getEnd() - parentShard.getStart()) / numberOfChildren;

            if(rangeSize <= MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException("Cannot split shard [" + splitShardId + "] further.");
            }

            long start = parentShard.getStart();
            List<ShardRange> newChildShardsList = new ArrayList<>();
            int nextChildShardId = maxShardId + 1;
            for (int i = 0; i < numberOfChildren; ++i) {
                long end = i == numberOfChildren - 1 ? parentShard.getEnd() : start + rangeSize;
                int childShardId = nextChildShardId++;
                ShardRange childShard = new ShardRange(childShardId, (int) start, (int) end);
                newChildShardsList.add(childShard);
                start = end + 1;
            }
            ShardRange[] newShardRanges = newChildShardsList.toArray(new ShardRange[0]);
            validateShardRanges(splitShardId, newShardRanges);

            parentToChildShards.put(splitShardId, newShardRanges);
        }

        public void updateSplitMetadataForChildShards(int sourceShardId, Set<Integer> newChildShardIds) {
            Tuple<Integer, ShardRange> shardRangeTuple = findRootAndShard(sourceShardId, rootShardsToAllChildren);
            assert shardRangeTuple != null;

            assert newChildShardIds.size() == parentToChildShards.get(sourceShardId).length;
            for (ShardRange childShard : parentToChildShards.get(sourceShardId)) {
                assert newChildShardIds.contains(childShard.getShardId());
            }

            List<ShardRange> shardsUnderRoot = rootShardsToAllChildren[shardRangeTuple.v1()] == null ? new ArrayList<>() :
                Arrays.asList(rootShardsToAllChildren[shardRangeTuple.v1()]);
            shardsUnderRoot.remove(shardRangeTuple.v2());
            shardsUnderRoot.addAll(Arrays.asList(parentToChildShards.get(sourceShardId)));
            ShardRange[] newShardsUnderRoot = shardsUnderRoot.toArray(new ShardRange[0]);
            Arrays.sort(newShardsUnderRoot);
            validateShardRanges(shardRangeTuple.v1(), newShardsUnderRoot);

            maxShardId += newChildShardIds.size();
            rootShardsToAllChildren[shardRangeTuple.v1()] = newShardsUnderRoot;
            inProgressSplitShardId = SPLIT_NOT_IN_PROGRESS;
        }

        public void cancelSplit(int sourceShardId) {
            assert inProgressSplitShardId != SPLIT_NOT_IN_PROGRESS;
            parentToChildShards.remove(sourceShardId);
            inProgressSplitShardId = SPLIT_NOT_IN_PROGRESS;
        }

        public SplitShardsMetadata build() {
            return new SplitShardsMetadata(this.rootShardsToAllChildren, this.parentToChildShards,
                this.inProgressSplitShardId, this.maxShardId);
        }
    }

    private static Tuple<Integer, ShardRange> findRootAndShard(int shardId, ShardRange[][] rootShardsToAllChildren) {
        ShardRange[] allChildren;
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            allChildren = rootShardsToAllChildren[rootShardId];
            if (allChildren != null) {
                for (ShardRange shardUnderRoot : allChildren) {
                    if (shardUnderRoot.getShardId() == shardId) {
                        return new Tuple<>(rootShardId, shardUnderRoot);
                    }
                }
            }
        }

        if (shardId < rootShardsToAllChildren.length && rootShardsToAllChildren[shardId] == null) {
            // We are splitting a root shard in this case.
            return new Tuple<>(shardId, new ShardRange(shardId, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }

        throw new IllegalArgumentException("Shard ID doesn't exist in the current list of shard ranges");
    }

    public int getInProgressSplitShardId() {
        return inProgressSplitShardId;
    }

    public boolean isSplitOfShardInProgress(int shardId) {
        return inProgressSplitShardId == shardId;
    }

    public boolean isEmptyParentShard(int shardId) {
        return isSplitOfShardInProgress(shardId) == false && parentToChildShards.containsKey(shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardsMetadata)) return false;

        SplitShardsMetadata that = (SplitShardsMetadata) o;

        if (maxShardId != that.maxShardId) return false;
        if (inProgressSplitShardId != that.inProgressSplitShardId) return false;
        if (!Arrays.deepEquals(rootShardsToAllChildren, that.rootShardsToAllChildren)) return false;
        return parentToChildShards.equals(that.parentToChildShards);
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(rootShardsToAllChildren);
        result = 31 * result + parentToChildShards.hashCode();
        result = 31 * result + maxShardId;
        result = 31 * result + inProgressSplitShardId;
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_NUMBER_OF_ROOT_SHARDS, rootShardsToAllChildren.length);
        builder.field(KEY_MAX_SHARD_ID, maxShardId);
        builder.field(KEY_IN_PROGRESS_SPLIT_SHARD_ID, inProgressSplitShardId);
        builder.startObject(KEY_ROOT_SHARDS_TO_ALL_CHILDREN);
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            ShardRange[] childShards = rootShardsToAllChildren[rootShardId];
            if (childShards != null) {
                builder.startArray(String.valueOf(rootShardId));
                for (ShardRange childShard : childShards) {
                    childShard.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        builder.endObject();

        builder.startObject(KEY_TEMP_SHARD_ID_TO_CHILD_SHARDS);
        for (Integer parentShardId : parentToChildShards.keySet()) {
            builder.startArray(String.valueOf(parentShardId));
            for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
                childShard.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    public static SplitShardsMetadata parse(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        int maxShardId = -1, inProgressSplitShardId = SPLIT_NOT_IN_PROGRESS;
        ShardRange[][] rootShardsToAllChildren = null;
        Map<Integer, ShardRange[]> tempShardIdToChildShards = new HashMap<>();
        int numberOfRootShards = -1;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (KEY_MAX_SHARD_ID.equals(currentFieldName)) {
                    maxShardId = parser.intValue();
                } else if (KEY_IN_PROGRESS_SPLIT_SHARD_ID.equals(currentFieldName)) {
                    inProgressSplitShardId = parser.intValue();
                } else if (KEY_NUMBER_OF_ROOT_SHARDS.equals(currentFieldName)) {
                    numberOfRootShards = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_ROOT_SHARDS_TO_ALL_CHILDREN.equals(currentFieldName)) {
                    Map<Integer, ShardRange[]> rootShards = parseShardsMap(parser);
                    rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
                    for (Map.Entry<Integer, ShardRange[]> entry : rootShards.entrySet()) {
                        rootShardsToAllChildren[entry.getKey()] = entry.getValue();
                    }
                } else if (KEY_TEMP_SHARD_ID_TO_CHILD_SHARDS.equals(currentFieldName)) {
                    tempShardIdToChildShards = parseShardsMap(parser);
                }
            }
        }

        return new SplitShardsMetadata(rootShardsToAllChildren, tempShardIdToChildShards, inProgressSplitShardId, maxShardId);
    }

    private static Map<Integer, ShardRange[]> parseShardsMap(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Map<Integer, ShardRange[]> shardsMap = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<ShardRange> childShardRanges = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    ShardRange shardRange = ShardRange.parse(parser);
                    childShardRanges.add(shardRange);
                }
                assert currentFieldName != null;
                Integer parentShard = Integer.parseInt(currentFieldName);
                shardsMap.put(parentShard, childShardRanges.toArray(new ShardRange[0]));
            }
        }

        return shardsMap;
    }

    public static Diff<SplitShardsMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardsMetadata::new, in);
    }

}
