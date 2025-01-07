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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * indexRoutings like id, state, version, etc.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardRouting implements Writeable, ToXContentObject {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;

    private final ShardId shardId;
    private final String currentNodeId;
    private final String relocatingNodeId;
    private final boolean primary;
    private final ShardRoutingState state;
    private final RecoverySource recoverySource;
    private final UnassignedInfo unassignedInfo;
    private final AllocationId allocationId;
    private final transient List<ShardRouting> asList;
    private final long expectedShardSize;
    @Nullable
    private final ShardRouting targetRelocatingShard;
    @Nullable
    private final ShardRouting[] recoveringChildShards;
    @Nullable
    private final ShardId parentShardId;

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    protected ShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo,
        AllocationId allocationId,
        long expectedShardSize,
        ShardRouting[] recoveringChildShards,
        ShardId parentShardId
    ) {
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.state = state;
        this.recoverySource = recoverySource;
        this.unassignedInfo = unassignedInfo;
        this.allocationId = allocationId;
        this.expectedShardSize = expectedShardSize;
        this.targetRelocatingShard = initializeTargetRelocatingShard();
        this.recoveringChildShards = recoveringChildShards;
        this.parentShardId = parentShardId;
        this.asList = Collections.singletonList(this);
        assert expectedShardSize == UNAVAILABLE_EXPECTED_SHARD_SIZE
            || state == ShardRoutingState.INITIALIZING
            || state == ShardRoutingState.RELOCATING
            || state == ShardRoutingState.SPLITTING : expectedShardSize + " state: " + state;
        assert expectedShardSize >= 0
            || state != ShardRoutingState.INITIALIZING
            || state != ShardRoutingState.RELOCATING
            || state != ShardRoutingState.SPLITTING : expectedShardSize + " state: " + state;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
        assert (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) == (recoverySource != null)
            : "recovery source only available on unassigned or initializing shard but was " + state;
        assert recoverySource == null || recoverySource == PeerRecoverySource.INSTANCE ||
            recoverySource == RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE || primary
            : "replica shards always recover from primary or child shards";
        assert (currentNodeId == null) == (state == ShardRoutingState.UNASSIGNED) : "unassigned shard must not be assigned to a node "
            + this;
    }

    @Nullable
    private ShardRouting initializeTargetRelocatingShard() {
        if (state == ShardRoutingState.RELOCATING) {
            return new ShardRouting(
                shardId,
                relocatingNodeId,
                currentNodeId,
                primary,
                ShardRoutingState.INITIALIZING,
                PeerRecoverySource.INSTANCE,
                unassignedInfo,
                AllocationId.newTargetRelocation(allocationId),
                expectedShardSize,
                null,
                null
            );
        } else {
            return null;
        }
    }

    public List<ShardRouting> assignChildShards(Map<ShardRouting, String> assignedRoutingNodes) {
        List<ShardRouting> assignedChildShards = new ArrayList<>();
        int idx = 0;
        for (ShardRouting childShard : assignedRoutingNodes.keySet()) {
            ShardRouting assignedChildShard = new ShardRouting(
                childShard.shardId,
                assignedRoutingNodes.get(childShard),
                null,
                childShard.primary,
                ShardRoutingState.INITIALIZING,
                childShard.recoverySource,
                unassignedInfo,
                childShard.allocationId,
                expectedShardSize,
                null,
                shardId
            );
            assignedChildShards.add(assignedChildShard);
            recoveringChildShards[idx++] = assignedChildShard;
        }

        return assignedChildShards;
    }

    public ShardRouting updatedStartedReplicaOnParent(ShardRouting initializingReplica, ShardRouting startedReplica) {
        List<ShardRouting> updatedChildShards = new ArrayList<>();
        boolean moved = false;
        for (ShardRouting childShard : recoveringChildShards) {
            if (childShard.equals(initializingReplica)) {
                assert startedReplica.primary == false;
                moved = true;
                updatedChildShards.add(startedReplica);
            } else {
                updatedChildShards.add(childShard);
            }
        }

        assert moved;
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            updatedChildShards.toArray(new ShardRouting[0]),
            parentShardId
        );
    }

    /**
     * Creates a new unassigned shard.
     */
    public static ShardRouting newUnassigned(
        ShardId shardId,
        boolean primary,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo
    ) {
        return new ShardRouting(
            shardId,
            null,
            null,
            primary,
            ShardRoutingState.UNASSIGNED,
            recoverySource,
            unassignedInfo,
            null,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );
    }

    public Index index() {
        return shardId.getIndex();
    }

    /**
     * The index name.
     */
    public String getIndexName() {
        return shardId.getIndexName();
    }

    /**
     * The shard id.
     */
    public int id() {
        return shardId.id();
    }

    /**
     * The shard id.
     */
    public int getId() {
        return id();
    }

    /**
     * The shard is unassigned (not allocated to any node).
     */
    public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently
     * {@link ShardRoutingState#STARTED started} or
     * {@link ShardRoutingState#RELOCATING relocating} to another node or
     * {@link ShardRoutingState#SPLITTING splitting}
     * Otherwise <code>false</code>
     */
    public boolean active() {
        return started() || relocating() || splitting();
    }

    /**
     * The shard is in started mode.
     */
    public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently being split.
     * Otherwise <code>false</code>
     *
     * @see ShardRoutingState#SPLITTING
     */
    public boolean splitting() {
        return state == ShardRoutingState.SPLITTING;
    }

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    public boolean assignedToNode() {
        return currentNodeId != null;
    }

    /**
     * The current node id the shard is allocated on.
     */
    public String currentNodeId() {
        return this.currentNodeId;
    }

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    /**
     * Returns a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting getTargetRelocatingShard() {
        assert relocating();
        return targetRelocatingShard;
    }

    /**
     * Additional metadata on why the shard is/was unassigned. The metadata is kept around
     * until the shard moves to STARTED.
     */
    @Nullable
    public UnassignedInfo unassignedInfo() {
        return unassignedInfo;
    }

    /**
     * An id that uniquely identifies an allocation.
     */
    @Nullable
    public AllocationId allocationId() {
        return this.allocationId;
    }

    /**
     * Returns <code>true</code> iff this shard is a primary.
     */
    public boolean primary() {
        return this.primary;
    }

    /**
     * The shard state.
     */
    public ShardRoutingState state() {
        return this.state;
    }

    /**
     * The shard id.
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * A shard iterator with just this shard in it.
     */
    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, asList);
    }

    public ShardRouting(ShardId shardId, StreamInput in) throws IOException {
        this.shardId = shardId;
        currentNodeId = in.readOptionalString();
        relocatingNodeId = in.readOptionalString();
        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource = RecoverySource.readFrom(in);
        } else {
            recoverySource = null;
        }
        unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        allocationId = in.readOptionalWriteable(AllocationId::new);
        final long shardSize;
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.SPLITTING) {
            shardSize = in.readLong();
        } else {
            shardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
        expectedShardSize = shardSize;
        asList = Collections.singletonList(this);
        targetRelocatingShard = initializeTargetRelocatingShard();
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            recoveringChildShards = in.readOptionalArray(ShardRouting::new, ShardRouting[]::new);
            parentShardId = in.readOptionalWriteable(ShardId::new);
        } else {
            recoveringChildShards = null;
            parentShardId = null;
        }
    }

    public ShardRouting(StreamInput in) throws IOException {
        this(new ShardId(in), in);
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeOptionalString(currentNodeId);
        out.writeOptionalString(relocatingNodeId);
        out.writeBoolean(primary);
        if (out.getVersion().onOrAfter(Version.V_3_0_0) || state != ShardRoutingState.SPLITTING) {
            out.writeByte(state.value());
        } else {
            throw new IllegalStateException("In-place split not allowed on older versions.");
        }
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource.writeTo(out);
        }
        out.writeOptionalWriteable(unassignedInfo);
        out.writeOptionalWriteable(allocationId);
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.SPLITTING) {
            out.writeLong(expectedShardSize);
        }
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeOptionalArray(recoveringChildShards);
            out.writeOptionalWriteable(parentShardId);
        } else {
            if (recoveringChildShards != null || parentShardId != null) {
                // In-progress shard split is not allowed in a mixed cluster where node(s) with an unsupported split
                // version is present. Hence, we also don't want to allow a node with an unsupported version
                // to get this state while shard split is in-progress.
                throw new IllegalStateException("In-place split not allowed on older versions.");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        writeToThin(out);
    }

    public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource) {
        assert this.unassignedInfo != null : "can only update unassign info if they are already set";
        assert this.unassignedInfo.isDelayed() || (unassignedInfo.isDelayed() == false) : "cannot transition from non-delayed to delayed";
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            null,
            parentShardId
        );
    }

    /**
     * Moves the shard to unassigned state.
     */
    public ShardRouting moveToUnassigned(UnassignedInfo unassignedInfo) {
        assert state != ShardRoutingState.UNASSIGNED : this;
        final RecoverySource recoverySource;
        if (active()) {
            if (primary()) {
                recoverySource = ExistingStoreRecoverySource.INSTANCE;
            } else {
                recoverySource = PeerRecoverySource.INSTANCE;
            }
        } else {
            recoverySource = recoverySource();
        }
        return new ShardRouting(
            shardId,
            null,
            null,
            primary,
            ShardRoutingState.UNASSIGNED,
            recoverySource,
            unassignedInfo,
            null,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
    }

    /**
     * Initializes an unassigned shard on a node.
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     */
    public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize) {
        assert state == ShardRoutingState.UNASSIGNED : this;
        assert relocatingNodeId == null : this;
        final AllocationId allocationId;
        if (existingAllocationId == null) {
            allocationId = AllocationId.newInitializing();
        } else {
            allocationId = AllocationId.newInitializing(existingAllocationId);
        }
        return new ShardRouting(
            shardId,
            nodeId,
            null,
            primary,
            ShardRoutingState.INITIALIZING,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            null,
            parentShardId
        );
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard
     */
    public ShardRouting relocate(String relocatingNodeId, long expectedShardSize) {
        assert state == ShardRoutingState.STARTED : "current shard has to be started in order to be relocated " + this;
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            ShardRoutingState.RELOCATING,
            recoverySource,
            null,
            AllocationId.newRelocation(allocationId),
            expectedShardSize,
            null,
            parentShardId
        );
    }

    /**
     * Create child shard routings for a splitting parent.
     */
    public ShardRouting createRecoveringChildShards(ShardRange[] recoveringChildShardRanges, int replicaCount) {
        int totalShards = recoveringChildShardRanges.length * (replicaCount + 1);
        AllocationId allocationId = AllocationId.newSplit(allocationId(), totalShards);
        ShardRouting[] childShards = new ShardRouting[totalShards];
        int allocationIdx = 0;
        UnassignedInfo childUnassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CHILD_SHARD_CREATED,
            "child_shard_allocation_pending[parent shard " + shardId + "]");
        for (ShardRange recoveringChildShardRange : recoveringChildShardRanges) {
            childShards[allocationIdx] = new ShardRouting(
                    new ShardId(index(), recoveringChildShardRange.getShardId()),
                    null,
                    null,
                    true,
                    ShardRoutingState.UNASSIGNED,
                    RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
                    childUnassignedInfo,
                    AllocationId.newTargetSplit(allocationId, allocationId.getSplitChildAllocationIds().get(allocationIdx)),
                    UNAVAILABLE_EXPECTED_SHARD_SIZE,
                    null,
                    shardId
            );
            allocationIdx++;

            for (int replica = 0; replica < replicaCount; replica++) {
                childShards[allocationIdx] = new ShardRouting(
                        new ShardId(index(), recoveringChildShardRange.getShardId()),
                        null,
                        null,
                        false,
                        ShardRoutingState.UNASSIGNED,
                        PeerRecoverySource.INSTANCE,
                        childUnassignedInfo,
                        AllocationId.newTargetSplit(allocationId, allocationId.getSplitChildAllocationIds().get(allocationIdx)),
                        UNAVAILABLE_EXPECTED_SHARD_SIZE,
                        null,
                        shardId
                );
                allocationIdx++;
            }
        }

        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.SPLITTING,
            recoverySource,
            null,
            allocationId,
            expectedShardSize,
            childShards,
            null
        );
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     */
    public ShardRouting cancelRelocation() {
        assert state == ShardRoutingState.RELOCATING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.STARTED,
            recoverySource,
            null,
            AllocationId.cancelRelocation(allocationId),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
    }

    /**
     * Cancel split of a shard. The shards state must be set
     * to <code>SPLITTING</code>.
     */
    public ShardRouting cancelSplit() {
        assert state == ShardRoutingState.SPLITTING : this;
        assert assignedToNode() : this;
        assert recoveringChildShards != null && recoveringChildShards.length > 0 : this;
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.STARTED,
            recoverySource,
            null,
            AllocationId.cancelSplit(allocationId),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );
    }

    /**
     * Removes relocation source of a non-primary shard. The shard state must be <code>INITIALIZING</code>.
     * This allows the non-primary shard to continue recovery from the primary even though its non-primary
     * relocation source has failed.
     */
    public ShardRouting removeRelocationSource() {
        assert primary == false : this;
        assert state == ShardRoutingState.INITIALIZING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            recoverySource,
            unassignedInfo,
            AllocationId.finishRelocation(allocationId),
            expectedShardSize,
            null,
            parentShardId
        );
    }

    /**
     * Reinitializes a replica shard, giving it a fresh allocation id
     */
    public ShardRouting reinitializeReplicaShard() {
        assert state == ShardRoutingState.INITIALIZING : this;
        assert primary == false : this;
        assert isRelocationTarget() == false : this;
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.INITIALIZING,
            recoverySource,
            unassignedInfo,
            AllocationId.newInitializing(),
            expectedShardSize,
            null,
            parentShardId
        );
    }

    public ShardRouting moveChildReplicaToStarted() {
        // Note that we don't detach parent info yet. This will be done when primary child shards are started so that
        // started child replicas continue to act as child shards.
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.STARTED,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code> or <code>SPLITTING</code>. Any relocation will be
     * canceled.
     */
    public ShardRouting moveToStarted() {
        assert state == ShardRoutingState.INITIALIZING : "expected an initializing shard " + this;
        AllocationId allocationId = this.allocationId;
        if (allocationId.getRelocationId() != null) {
            // relocation target
            allocationId = AllocationId.finishRelocation(allocationId);
        } else if (allocationId.getParentAllocationId() != null) {
            // split target
            allocationId = AllocationId.finishSplit(allocationId);
        }
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.STARTED,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );
    }

    /**
     * Removes Parent info from a started replica
     */
    public ShardRouting removeParentFromReplica() {
        assert isStartedChildReplica();
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            ShardRoutingState.STARTED,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );
    }

    /**
     * Make the active primary shard as replica
     *
     * @throws IllegalShardRoutingStateException if shard is already a replica
     */
    public ShardRouting moveActivePrimaryToReplica() {
        assert active() : "expected an active shard " + this;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not a primary shard, can't move to replica");
        }
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            false,
            state,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            recoveringChildShards,
            parentShardId
        );
    }

    /**
     * Make the active shard primary unless it's not primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a primary
     */
    public ShardRouting moveActiveReplicaToPrimary() {
        assert active() : "expected an active shard " + this;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            true,
            state,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            recoveringChildShards,
            parentShardId
        );
    }

    /**
     * Set the unassigned primary shard to non-primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a replica
     */
    public ShardRouting moveUnassignedFromPrimary() {
        assert state == ShardRoutingState.UNASSIGNED : "expected an unassigned shard " + this;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            false,
            state,
            PeerRecoverySource.INSTANCE,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            recoveringChildShards,
            parentShardId
        );
    }

    /**
     * returns true if this routing has the same allocation ID as another.
     * <p>
     * Note: if both shard routing has a null as their {@link #allocationId()}, this method returns false as the routing describe
     * no allocation at all..
     **/
    public boolean isSameAllocation(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && this.allocationId.getId().equals(other.allocationId.getId());
        assert b == false || this.currentNodeId.equals(other.currentNodeId)
            : "ShardRoutings have the same allocation id but not the same node. This [" + this + "], other [" + other + "]";
        return b;
    }

    /**
     * Returns <code>true</code> if this shard is a relocation target for another shard
     * (i.e., was created with {@link #initializeTargetRelocatingShard()}
     */
    public boolean isRelocationTarget() {
        return state == ShardRoutingState.INITIALIZING && relocatingNodeId != null;
    }

    /**
     * Returns <code>true</code> if this shard is a split target for another shard
     * (i.e., was created in {@link #assignChildShards(Map)} ()} ()}
     */
    public boolean isSplitTarget() {
        return getParentShardId() != null;
    }

    /**
     * returns true if the routing is the split target of the given routing
     */
    public boolean isSplitTargetOf(ShardRouting other) {

        boolean b = this.allocationId != null
            && other.allocationId != null
            && this.allocationId.getParentAllocationId() != null
            && this.allocationId.getParentAllocationId().equals(other.allocationId.getId());

        assert b == false || other.state == ShardRoutingState.SPLITTING
            : "ShardRouting is a split target but the source shard state isn't splitting. This [" + this + "], other [" + other + "]";

        assert b == false
            || other.allocationId.getSplitChildAllocationIds() != null
                && other.allocationId.getSplitChildAllocationIds().contains(this.allocationId.getId())
            : "ShardRouting is a splitting target but the source split allocation doesn't contain this allocationId.getId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.getParentShardId().equals(other.shardId())
            : "ShardRouting is a splitting target but current splitting shard id isn't equal to source shard id."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || other.primary : "ShardRouting is a splitting target but primary flag is different."
            + " This ["
            + this
            + "], target ["
            + other
            + "]";

        return b;
    }

    /** returns true if the routing is the relocation target of the given routing */
    public boolean isRelocationTargetOf(ShardRouting other) {
        boolean b = this.allocationId != null
            && other.allocationId != null
            && this.state == ShardRoutingState.INITIALIZING
            && this.allocationId.getId().equals(other.allocationId.getRelocationId());

        assert b == false || other.state == ShardRoutingState.RELOCATING
            : "ShardRouting is a relocation target but the source shard state isn't relocating. This [" + this + "], other [" + other + "]";

        assert b == false || other.allocationId.getId().equals(this.allocationId.getRelocationId())
            : "ShardRouting is a relocation target but the source id isn't equal to source's allocationId.getRelocationId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId)
            : "ShardRouting is a relocation target but source current node id isn't equal to target relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId)
            : "ShardRouting is a relocation target but current node id isn't equal to source relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.shardId.equals(other.shardId)
            : "ShardRouting is a relocation target but both indexRoutings are not of the same shard id."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.primary == other.primary : "ShardRouting is a relocation target but primary flag is different."
            + " This ["
            + this
            + "], target ["
            + other
            + "]";

        return b;
    }

    public boolean isSplitSourceOf(ShardRouting other) {
        boolean b = this.allocationId != null
            && other.allocationId != null
            && this.state == ShardRoutingState.SPLITTING
            && other.allocationId.getParentAllocationId() != null
            && this.allocationId.getId().equals(other.allocationId.getParentAllocationId());

        assert b == false
            || this.allocationId.getSplitChildAllocationIds() != null
                && this.allocationId.getSplitChildAllocationIds().contains(other.allocationId.getId())
            : "ShardRouting is a splitting source but its split allocation ids doesn't contain target allocationId.getId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.shardId.equals(other.getParentShardId())
            : "ShardRouting is a splitting source but current shard id isn't equal to target splitting shard id."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.primary : "ShardRouting is a splitting source but primary flag is different."
            + " This ["
            + this
            + "], target ["
            + other
            + "]";

        return b;
    }

    /** returns true if the routing is the relocation source for the given routing */
    public boolean isRelocationSourceOf(ShardRouting other) {
        boolean b = this.allocationId != null
            && other.allocationId != null
            && other.state == ShardRoutingState.INITIALIZING
            && other.allocationId.getId().equals(this.allocationId.getRelocationId());

        assert b == false || this.state == ShardRoutingState.RELOCATING
            : "ShardRouting is a relocation source but shard state isn't relocating. This [" + this + "], other [" + other + "]";

        assert b == false || this.allocationId.getId().equals(other.allocationId.getRelocationId())
            : "ShardRouting is a relocation source but the allocation id isn't equal to other.allocationId.getRelocationId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId)
            : "ShardRouting is a relocation source but current node isn't equal to other's relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId)
            : "ShardRouting is a relocation source but relocating node isn't equal to other's current node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.splitting() || this.shardId.equals(other.shardId)
            : "ShardRouting is a relocation source but both indexRoutings are not of the same shard."
                + " This ["
                + this
                + "], target ["
                + other
                + "]";

        assert b == false || this.primary == other.primary : "ShardRouting is a relocation source but primary flag is different. This ["
            + this
            + "], target ["
            + other
            + "]";

        return b;
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., unassigned info */
    public boolean equalsIgnoringMetadata(ShardRouting other) {
        if (primary != other.primary) {
            return false;
        }
        if (shardId != null ? !shardId.equals(other.shardId) : other.shardId != null) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(other.currentNodeId) : other.currentNodeId != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(other.relocatingNodeId) : other.relocatingNodeId != null) {
            return false;
        }
        if (allocationId != null ? !allocationId.equals(other.allocationId) : other.allocationId != null) {
            return false;
        }
        if (state != other.state) {
            return false;
        }
        if (recoverySource != null ? !recoverySource.equals(other.recoverySource) : other.recoverySource != null) {
            return false;
        }
        if (Arrays.equals(recoveringChildShards, other.recoveringChildShards) == false) {
            return false;
        }
        if (Objects.equals(parentShardId, other.parentShardId) == false) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        if (unassignedInfo != null ? !unassignedInfo.equals(that.unassignedInfo) : that.unassignedInfo != null) {
            return false;
        }
        return equalsIgnoringMetadata(that);
    }

    /**
     * Cache hash code in the same way as {@link String#hashCode()}) using racy single-check idiom
     * as it is mainly used in single-threaded code ({@link BalancedShardsAllocator}).
     */
    private int hashCode; // default to 0

    @Override
    public int hashCode() {
        int h = hashCode;
        if (h == 0) {
            h = shardId.hashCode();
            h = 31 * h + (currentNodeId != null ? currentNodeId.hashCode() : 0);
            h = 31 * h + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
            h = 31 * h + (primary ? 1 : 0);
            h = 31 * h + (state != null ? state.hashCode() : 0);
            h = 31 * h + (recoverySource != null ? recoverySource.hashCode() : 0);
            h = 31 * h + (allocationId != null ? allocationId.hashCode() : 0);
            h = 31 * h + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
            h = 31 * h + (recoveringChildShards != null ? Arrays.hashCode(recoveringChildShards) : 0);
            h = 31 * h + (parentShardId != null ? parentShardId.hashCode() : 0);
            hashCode = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    /**
     * A short description of the shard.
     */
    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(shardId.getIndexName()).append(']').append('[').append(shardId.getId()).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (recoverySource != null) {
            sb.append(", recovery_source[").append(recoverySource).append("]");
        }
        sb.append(", s[").append(state).append("]");
        if (allocationId != null) {
            sb.append(", a").append(allocationId);
        }
        if (this.unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo.toString());
        }
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            sb.append(", expected_shard_size[").append(expectedShardSize).append("]");
        }
        if (recoveringChildShards != null) {
            sb.append(", recovering_child_shards [").append(Arrays.toString(recoveringChildShards)).append("]");
        }
        if (parentShardId != null) {
            sb.append(", parent_shard_id [").append(parentShardId).append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("state", state())
            .field("primary", primary())
            .field("node", currentNodeId())
            .field("relocating_node", relocatingNodeId())
            .field("shard", id())
            .field("index", getIndexName());
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        if (recoverySource != null) {
            builder.field("recovery_source", recoverySource);
        }
        if (allocationId != null) {
            builder.field("allocation_id");
            allocationId.toXContent(builder, params);
        }
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        if (recoveringChildShards != null) {
            builder.startArray("recovering_child_shards");
            for (ShardRouting childShard : recoveringChildShards) {
                childShard.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (parentShardId != null) {
            builder.field("parent_shard_id");
            parentShardId.toXContent(builder, params);
        }

        return builder.endObject();
    }

    /**
     * Returns the expected shard size for {@link ShardRoutingState#RELOCATING}, {@link ShardRoutingState#INITIALIZING}
     * and {@link ShardRoutingState#SPLITTING} shards. If it's size is not available {@value #UNAVAILABLE_EXPECTED_SHARD_SIZE}
     * will be returned.
     */
    public long getExpectedShardSize() {
        return expectedShardSize;
    }

    /**
     * Returns recovery source for the given shard. Replica shards always recover from the primary {@link PeerRecoverySource}.
     *
     * @return recovery source or null if shard is {@link #active()}
     */
    @Nullable
    public RecoverySource recoverySource() {
        return recoverySource;
    }

    public boolean unassignedReasonIndexCreated() {
        if (unassignedInfo != null) {
            return unassignedInfo.getReason() == UnassignedInfo.Reason.INDEX_CREATED;
        }
        return false;
    }

    public boolean unassignedReasonChildShardCreated() {
        if (unassignedInfo != null) {
            return unassignedInfo.getReason() == UnassignedInfo.Reason.CHILD_SHARD_CREATED;
        }
        return false;
    }

    @Nullable
    public ShardRouting[] getRecoveringChildShards() {
        // Not returning copy because in RoutingNodes shards are removed based on their object identity.
        return recoveringChildShards;
    }

    public ShardId getParentShardId() {
        return parentShardId;
    }

    public boolean isStartedChildReplica() {
        return primary == false && getParentShardId() != null && started();
    }
}
