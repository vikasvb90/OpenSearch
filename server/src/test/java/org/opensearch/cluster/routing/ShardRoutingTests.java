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
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import java.util.*;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.opensearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;

import org.opensearch.cluster.metadata.ShardRange;
//import org.opensearch.cluster.routing.allocation.AllocationId;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;

//import org.junit.jupiter.api.Test;
//import static org.junit.jupiter.api.Assertions.*;

//import org.junit.jupiter.api.Test;
//import org.opensearch.cluster.routing.*;
//import static org.hamcrest.Matchers.*;
//import static org.junit.jupiter.api.Assertions.*;
//import org.opensearch.cluster.routing.allocation.AllocationId;

public class ShardRoutingTests extends OpenSearchTestCase {

    public void testIsSameAllocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting unassignedShard1 = TestShardRouting.newShardRouting("test", 1, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 = TestShardRouting.newShardRouting("test", 0, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting("test", 1, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        ShardRouting startedShard1 = initializingShard1.moveToStarted();

        // test identity
        assertTrue(initializingShard0.isSameAllocation(initializingShard0));

        // test same allocation different state
        assertTrue(initializingShard0.isSameAllocation(startedShard0));

        // test unassigned is false even to itself
        assertFalse(unassignedShard0.isSameAllocation(unassignedShard0));

        // test different shards/nodes/state
        assertFalse(unassignedShard0.isSameAllocation(unassignedShard1));
        assertFalse(unassignedShard0.isSameAllocation(initializingShard0));
        assertFalse(unassignedShard0.isSameAllocation(initializingShard1));
        assertFalse(unassignedShard0.isSameAllocation(startedShard1));
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(
            index,
            shard,
            state == ShardRoutingState.UNASSIGNED ? null : "1",
            state == ShardRoutingState.RELOCATING ? "2" : null,
            state != ShardRoutingState.UNASSIGNED && randomBoolean(),
            state
        );
    }

    public void testIsSourceTargetRelocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 = TestShardRouting.newShardRouting(
            "test",
            0,
            "node1",
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting(
            "test",
            1,
            "node1",
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        assertFalse(initializingShard0.isRelocationTarget());
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        assertFalse(startedShard0.isRelocationTarget());
        assertFalse(initializingShard1.isRelocationTarget());
        ShardRouting startedShard1 = initializingShard1.moveToStarted();
        assertFalse(startedShard1.isRelocationTarget());
        ShardRouting sourceShard0a = startedShard0.relocate("node2", -1);
        assertFalse(sourceShard0a.isRelocationTarget());
        ShardRouting targetShard0a = sourceShard0a.getTargetRelocatingShard();
        assertTrue(targetShard0a.isRelocationTarget());
        ShardRouting sourceShard0b = startedShard0.relocate("node2", -1);
        ShardRouting sourceShard1 = startedShard1.relocate("node2", -1);

        // test true scenarios
        assertTrue(targetShard0a.isRelocationTargetOf(sourceShard0a));
        assertTrue(sourceShard0a.isRelocationSourceOf(targetShard0a));

        // test two shards are not mixed
        assertFalse(targetShard0a.isRelocationTargetOf(sourceShard1));
        assertFalse(sourceShard1.isRelocationSourceOf(targetShard0a));

        // test two allocations are not mixed
        assertFalse(targetShard0a.isRelocationTargetOf(sourceShard0b));
        assertFalse(sourceShard0b.isRelocationSourceOf(targetShard0a));

        // test different shard states
        assertFalse(targetShard0a.isRelocationTargetOf(unassignedShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(unassignedShard0));
        assertFalse(unassignedShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(unassignedShard0.isRelocationSourceOf(sourceShard0a));

        assertFalse(targetShard0a.isRelocationTargetOf(initializingShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(initializingShard0));
        assertFalse(initializingShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(initializingShard0.isRelocationSourceOf(sourceShard0a));

        assertFalse(targetShard0a.isRelocationTargetOf(startedShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(startedShard0));
        assertFalse(startedShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(startedShard0.isRelocationSourceOf(sourceShard0a));
    }

    public void testEqualsIgnoringVersion() {
        ShardRouting routing = randomShardRouting("test", 0);

        ShardRouting otherRouting = routing;

        Integer[] changeIds = new Integer[] { 0, 1, 2, 3, 4, 5, 6 };
        for (int changeId : randomSubsetOf(randomIntBetween(1, changeIds.length), changeIds)) {
            boolean unchanged = false;
            switch (changeId) {
                case 0:
                    // change index
                    ShardId shardId = new ShardId(new Index("blubb", randomAlphaOfLength(10)), otherRouting.id());
                    otherRouting = new ShardRouting(
                        shardId,
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary(),
                        otherRouting.state(),
                        otherRouting.recoverySource(),
                        otherRouting.unassignedInfo(),
                        otherRouting.allocationId(),
                        otherRouting.getExpectedShardSize()
                    );
                    break;
                case 1:
                    // change shard id
                    otherRouting = new ShardRouting(
                        new ShardId(otherRouting.index(), otherRouting.id() + 1),
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary(),
                        otherRouting.state(),
                        otherRouting.recoverySource(),
                        otherRouting.unassignedInfo(),
                        otherRouting.allocationId(),
                        otherRouting.getExpectedShardSize()
                    );
                    break;
                case 2:
                    // change current node
                    if (otherRouting.assignedToNode() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId() + "_1",
                            otherRouting.relocatingNodeId(),
                            otherRouting.primary(),
                            otherRouting.state(),
                            otherRouting.recoverySource(),
                            otherRouting.unassignedInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize()
                        );
                    }
                    break;
                case 3:
                    // change relocating node
                    if (otherRouting.relocating() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId() + "_1",
                            otherRouting.primary(),
                            otherRouting.state(),
                            otherRouting.recoverySource(),
                            otherRouting.unassignedInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize()
                        );
                    }
                    break;
                case 4:
                    // change recovery source (only works for inactive primaries)
                    if (otherRouting.active() || otherRouting.primary() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId(),
                            otherRouting.primary(),
                            otherRouting.state(),
                            new RecoverySource.SnapshotRecoverySource(
                                UUIDs.randomBase64UUID(),
                                new Snapshot("test", new SnapshotId("s1", UUIDs.randomBase64UUID())),
                                Version.CURRENT,
                                new IndexId("test", UUIDs.randomBase64UUID(random()))
                            ),
                            otherRouting.unassignedInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize()
                        );
                    }
                    break;
                case 5:
                    // change primary flag
                    otherRouting = TestShardRouting.newShardRouting(
                        otherRouting.getIndexName(),
                        otherRouting.id(),
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary() == false,
                        otherRouting.state(),
                        otherRouting.unassignedInfo()
                    );
                    break;
                case 6:
                    // change state
                    ShardRoutingState newState;
                    do {
                        newState = randomFrom(ShardRoutingState.values());
                    } while (newState == otherRouting.state());

                    UnassignedInfo unassignedInfo = otherRouting.unassignedInfo();
                    if (unassignedInfo == null
                        && (newState == ShardRoutingState.UNASSIGNED || newState == ShardRoutingState.INITIALIZING)) {
                        unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test");
                    }

                    otherRouting = TestShardRouting.newShardRouting(
                        otherRouting.getIndexName(),
                        otherRouting.id(),
                        newState == ShardRoutingState.UNASSIGNED
                            ? null
                            : (otherRouting.currentNodeId() == null ? "1" : otherRouting.currentNodeId()),
                        newState == ShardRoutingState.RELOCATING ? "2" : null,
                        otherRouting.primary(),
                        newState,
                        unassignedInfo
                    );
                    break;
            }

            if (randomBoolean()) {
                // change unassigned info
                otherRouting = TestShardRouting.newShardRouting(
                    otherRouting.getIndexName(),
                    otherRouting.id(),
                    otherRouting.currentNodeId(),
                    otherRouting.relocatingNodeId(),
                    otherRouting.primary(),
                    otherRouting.state(),
                    otherRouting.unassignedInfo() == null
                        ? new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
                        : new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, otherRouting.unassignedInfo().getMessage() + "_1")
                );
            }

            if (unchanged == false) {
                logger.debug("comparing\nthis  {} to\nother {}", routing, otherRouting);
                assertFalse(
                    "expected non-equality\nthis  " + routing + ",\nother " + otherRouting,
                    routing.equalsIgnoringMetadata(otherRouting)
                );
            }
        }
    }

    public void testSwapPrimaryWithReplica() {
        final ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        assertThrows(AssertionError.class, unassignedShard0::moveActivePrimaryToReplica);

        final ShardRouting activeShard0 = TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
        assertThrows(IllegalShardRoutingStateException.class, activeShard0::moveActivePrimaryToReplica);

        final ShardRouting activeShard1 = TestShardRouting.newShardRouting("test", 0, "node-1", true, ShardRoutingState.STARTED);
        final ShardRouting activeReplicaShard1 = activeShard1.moveActivePrimaryToReplica();
        assertFalse(activeReplicaShard1.primary());
    }

    public void testExpectedSize() throws IOException {
        final int iters = randomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            ShardRouting routing = randomShardRouting("test", 0);
            long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
            if (routing.unassigned()) {
                routing = ShardRoutingHelper.initialize(routing, "foo", byteSize);
            } else if (routing.started()) {
                routing = ShardRoutingHelper.relocate(routing, "foo", byteSize);
            } else {
                byteSize = -1;
            }
            if (randomBoolean()) {
                BytesStreamOutput out = new BytesStreamOutput();
                routing.writeTo(out);
                routing = new ShardRouting(out.bytes().streamInput());
            }
            if (routing.initializing() || routing.relocating()) {
                assertEquals(routing.toString(), byteSize, routing.getExpectedShardSize());
                if (byteSize >= 0) {
                    assertTrue(routing.toString(), routing.toString().contains("expected_shard_size[" + byteSize + "]"));
                }
                if (routing.initializing()) {
                    routing = routing.moveToStarted();
                    assertEquals(-1, routing.getExpectedShardSize());
                    assertFalse(routing.toString(), routing.toString().contains("expected_shard_size[" + byteSize + "]"));
                }
            } else {
                assertFalse(routing.toString(), routing.toString().contains("expected_shard_size [" + byteSize + "]"));
                assertEquals(byteSize, routing.getExpectedShardSize());
            }
        }
    }
    /**
     * Test case for assignChildShards method
     */
    public void test_assignChildShards_1() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.STARTED);

        // Create child shards
        ShardId childShardId1 = new ShardId("test_index", "_na_", 1);
        ShardId childShardId2 = new ShardId("test_index", "_na_", 2);
        ShardRouting childShard1 = TestShardRouting.newShardRouting(childShardId1, null, true, ShardRoutingState.UNASSIGNED);
        ShardRouting childShard2 = TestShardRouting.newShardRouting(childShardId2, null, false, ShardRoutingState.UNASSIGNED);

        // Create assigned routing nodes map
        Map<ShardRouting, String> assignedRoutingNodes = new HashMap<>();
        assignedRoutingNodes.put(childShard1, "node2");
        assignedRoutingNodes.put(childShard2, "node3");

        // Create a splitting shard
        ShardRouting splittingShard = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            null,
            parentShard.primary(),
            parentShard.isSearchOnly(),
            ShardRoutingState.SPLITTING,
            parentShard.recoverySource(),
            parentShard.unassignedInfo(),
            parentShard.allocationId(),
            1000L,
            new ShardRouting[]{childShard1, childShard2},
            null
        );

        // Act
        List<ShardRouting> result = splittingShard.assignChildShards(assignedRoutingNodes);

        // Assert
        assertNotNull("Result should not be null", result);
        assertEquals("Should assign correct number of child shards", 2, result.size());

        for (ShardRouting assignedChildShard : result) {
            assertTrue("Assigned child shard should be initializing", assignedChildShard.initializing());
            assertEquals("Assigned child shard should be in INITIALIZING state", ShardRoutingState.INITIALIZING, assignedChildShard.state());
            assertTrue("Assigned node should be in the assignedRoutingNodes map", assignedRoutingNodes.containsValue(assignedChildShard.currentNodeId()));
            assertEquals("Parent shard ID should match", parentShardId, assignedChildShard.getParentShardId());
        }

    }

    /**
     * Negative test cases for `assignChildShards`
     */
    public void test_assignChildShards_negative_tests() {
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.SPLITTING);

        // 1. Test with empty input
        Map<ShardRouting, String> emptyMap = Collections.emptyMap();
        List<ShardRouting> emptyResult = parentShard.assignChildShards(emptyMap);
        assertTrue("Result should be empty for empty input", emptyResult.isEmpty());

        // 2. Test with null input
        assertThrows(NullPointerException.class, () -> parentShard.assignChildShards(null));

        // 3. Test with incorrect input type
        Map<String, String> incorrectMap = new HashMap<>();
        incorrectMap.put("invalidKey", "invalidValue");
        assertThrows(ClassCastException.class, () -> parentShard.assignChildShards((Map) incorrectMap));

        // 4. Test with null child shard
        Map<ShardRouting, String> nullChildMap = new HashMap<>();
        nullChildMap.put(null, "node2");
        assertThrows(NullPointerException.class, () -> parentShard.assignChildShards(nullChildMap));
    }

    /**
     * Test case for updatedStartedReplicaOnParent method
     * Path constraints: (childShard.equals(initializingReplica))
     */
    public void testUpdatedStartedReplicaOnParent() {
        // Create a parent shard
        ShardId parentShardId = new ShardId("test", "_na_", 0);
        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.STARTED);

        // Create child shards
        ShardId childShardId1 = new ShardId("test", "_na_", 1);
        ShardId childShardId2 = new ShardId("test", "_na_", 2);

        ShardRouting initializingReplica = TestShardRouting.newShardRouting(childShardId1, "node2", false, ShardRoutingState.INITIALIZING);
        ShardRouting startedReplica = TestShardRouting.newShardRouting(childShardId1, "node2", false, ShardRoutingState.STARTED);
        ShardRouting otherChildShard = TestShardRouting.newShardRouting(childShardId2, "node3", false, ShardRoutingState.STARTED);

        // Create a parent shard with child shards
        ShardRouting parentWithChildren = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            parentShard.relocatingNodeId(),
            parentShard.primary(),
            parentShard.isSearchOnly(),
            parentShard.state(),
            parentShard.recoverySource(),
            parentShard.unassignedInfo(),
            parentShard.allocationId(),
            parentShard.getExpectedShardSize(),
            new ShardRouting[]{initializingReplica, otherChildShard},
            null
        );

        // Call the method under test
        ShardRouting updatedParent = parentWithChildren.updatedStartedReplicaOnParent(initializingReplica, startedReplica);

        // Assertions
        assertNotNull(updatedParent);
        assertEquals(parentShardId, updatedParent.shardId());
        assertEquals(parentShard.currentNodeId(), updatedParent.currentNodeId());
        assertEquals(parentShard.relocatingNodeId(), updatedParent.relocatingNodeId());
        assertEquals(parentShard.primary(), updatedParent.primary());
        assertEquals(parentShard.state(), updatedParent.state());

        ShardRouting[] updatedChildShards = updatedParent.getRecoveringChildShards();
        assertNotNull(updatedChildShards);
        assertEquals(2, updatedChildShards.length);

        // Check that the initializing replica was replaced with the started replica
        assertTrue(updatedChildShards[0].equals(startedReplica));
        assertTrue(updatedChildShards[1].equals(otherChildShard));
    }
    /**
     * Negative test cases for `updatedStartedReplicaOnParent`
     */
    public void test_updatedStartedReplicaOnParent_negative_cases() {
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId1 = new ShardId("test_index", "_na_", 1);
        ShardId childShardId2 = new ShardId("test_index", "_na_", 2);

        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.STARTED);
        ShardRouting initializingReplica = TestShardRouting.newShardRouting(childShardId1, "node2", false, ShardRoutingState.INITIALIZING);
        ShardRouting startedReplica = TestShardRouting.newShardRouting(childShardId1, "node2", false, ShardRoutingState.STARTED);

        ShardRouting[] childShards = new ShardRouting[]{
            initializingReplica,
            TestShardRouting.newShardRouting(childShardId2, "node3", false, ShardRoutingState.STARTED)
        };

        ShardRouting parentWithChildren = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            null,
            parentShard.primary(),
            parentShard.isSearchOnly(),
            parentShard.state(),
            parentShard.recoverySource(),
            parentShard.unassignedInfo(),
            parentShard.allocationId(),
            parentShard.getExpectedShardSize(),
            childShards,
            null
        );

        // 2. Test with initializing replica that doesn't exist in child shards
        ShardRouting nonExistentReplica = TestShardRouting.newShardRouting(
            new ShardId("test_index", "_na_", 3), "node4", false, ShardRoutingState.INITIALIZING);
        assertThrows(AssertionError.class, () ->
            parentWithChildren.updatedStartedReplicaOnParent(nonExistentReplica, startedReplica));

        // 3. Test with started replica that is primary
        ShardRouting primaryStartedReplica = TestShardRouting.newShardRouting(childShardId1, "node2", true, ShardRoutingState.STARTED);
        assertThrows(AssertionError.class, () ->
            parentWithChildren.updatedStartedReplicaOnParent(initializingReplica, primaryStartedReplica));

        // 5. Test with parent shard that has no child shards
        ShardRouting parentWithNoChildren = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            null,
            parentShard.primary(),
            parentShard.isSearchOnly(),
            parentShard.state(),
            parentShard.recoverySource(),
            parentShard.unassignedInfo(),
            parentShard.allocationId(),
            parentShard.getExpectedShardSize(),
            new ShardRouting[0],
            null
        );
        assertThrows(AssertionError.class, () ->
            parentWithNoChildren.updatedStartedReplicaOnParent(initializingReplica, startedReplica));
    }

    /**
     * Test cancelSplit when shard is not assigned to a node. We already have an assertion test for this hence it gives an assertion error
     */
    public void testCancelSplitWhenNotAssignedToNode() {

        assertThrows(AssertionError.class, () -> {
            TestShardRouting.newShardRouting("test", 0, null, true, ShardRoutingState.SPLITTING);
        });
    }

    /**
     * Test cancelSplit when shard is not in SPLITTING state
     */
    public void testCancelSplitWhenNotSplitting() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);

        assertThrows(AssertionError.class, () -> {
            shardRouting.cancelSplit();
        });
    }

    /**
     * Test cancelSplit when shard has no recovering child shards
     */
    public void testCancelSplitWithNoRecoveringChildShards() {
        ShardId shardId = new ShardId("test", "_na_", 0);
        ShardRouting shardRouting = new ShardRouting(
            shardId,
            "node1",
            null,
            true,
            false,
            ShardRoutingState.SPLITTING,
            null,
            null,
            AllocationId.newInitializing(),
            0,
            null,
            null
        );

        assertThrows(AssertionError.class, () -> {
            shardRouting.cancelSplit();
        });
    }

    /**
     * Test cancelSplit with null AllocationId
     */
    public void testCancelSplitWithNullAllocationId() {
        ShardId shardId = new ShardId("test", "_na_", 0);
        ShardRouting shardRouting = new ShardRouting(
            shardId,
            "node1",
            null,
            true,
            false,
            ShardRoutingState.SPLITTING,
            null,
            null,
            null,
            0,
            new ShardRouting[]{mock(ShardRouting.class)},
            null
        );

        assertThrows(NullPointerException.class, () -> {
            shardRouting.cancelSplit();
        });
    }

    /**
     * Test case for public ShardRouting cancelSplit()
     * This test verifies that the cancelSplit method correctly cancels the split operation
     * and returns a new ShardRouting with the expected properties.
     */
    public void test_cancelSplit_1() {
        // Arrange
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        String currentNodeId = "node1";
        boolean primary = true;
        boolean searchOnly = false;
        AllocationId allocationId = AllocationId.newSplit(AllocationId.newInitializing(), 2);

        // Mocking recoveringChildShards to satisfy the assert condition
        ShardRouting[] recoveringChildShards = new ShardRouting[]{
            TestShardRouting.newShardRouting(new ShardId("test_index", "_na_", 1), null, true, ShardRoutingState.UNASSIGNED)
        };

        ShardRouting splittingShard = new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            searchOnly,
            ShardRoutingState.SPLITTING,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            recoveringChildShards,
            null
        );

        // Act
        ShardRouting result = splittingShard.cancelSplit();

        // Assert
        assertNotNull(result);
        assertEquals(shardId, result.shardId());
        assertEquals(currentNodeId, result.currentNodeId());
        assertNull(result.relocatingNodeId());
        assertEquals(primary, result.primary());
        assertEquals(searchOnly, result.isSearchOnly());
        assertEquals(ShardRoutingState.STARTED, result.state());
        assertNull(result.recoverySource());
        assertNull(result.unassignedInfo());
        assertEquals(AllocationId.cancelSplit(allocationId), result.allocationId());
        assertEquals(UNAVAILABLE_EXPECTED_SHARD_SIZE, result.getExpectedShardSize());
        assertNull(result.getRecoveringChildShards());
        assertNull(result.getParentShardId());
    }

    public void testSplittingForDifferentStates() {
        ShardId shardId = new ShardId(new Index("test-index", UUID.randomUUID().toString()), 0);
        // Test STARTED state
        ShardRouting startedShard = TestShardRouting.newShardRouting(
            shardId,
            "node-1",
            true,
            ShardRoutingState.STARTED
        );
        assertFalse("Started shard should not be splitting", startedShard.splitting());

        // Test INITIALIZING state
        ShardRouting initializingShard = TestShardRouting.newShardRouting(
            shardId,
            "node-1",
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse("Initializing shard should not be splitting", initializingShard.splitting());

        // Test RELOCATING state
        ShardRouting relocatingShard = TestShardRouting.newShardRouting(
            shardId,
            "node-1",
            "node-2",
            true,
            ShardRoutingState.RELOCATING
        );
        assertFalse("Relocating shard should not be splitting", relocatingShard.splitting());

        // Test UNASSIGNED state
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        assertFalse("Unassigned shard should not be splitting", unassignedShard.splitting());

        // Create a shard in SPLITTING state
        ShardRouting splittingShard = TestShardRouting.newShardRouting(
            shardId,
            "node-1",
            null,
            true,
            ShardRoutingState.SPLITTING
        );

        assertTrue("Shard should be in splitting state", splittingShard.splitting());
    }

    public void testAssignChildShardsWithEmptyInput() {
        ShardRouting parentShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        Map<ShardRouting, String> emptyMap = Collections.emptyMap();

        List<ShardRouting> result = parentShard.assignChildShards(emptyMap);

        assertTrue("Result should be empty for empty input", result.isEmpty());
    }

    /**
     * Test case for createRecoveringChildShards method
     * This test verifies that the method correctly creates child shard routings for a splitting parent
     */
    public void testCreateRecoveringChildShards() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        String currentNodeId = "node1";
        boolean primary = true;
        boolean searchOnly = false;
        int replicaCount = 2;

        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, currentNodeId, primary, ShardRoutingState.STARTED);

        ShardRange[] recoveringChildShardRanges = new ShardRange[]{
            new ShardRange(1, 0, 50),
            new ShardRange(2, 51, 100)
        };

        // Act
        ShardRouting result = parentShard.createRecoveringChildShards(recoveringChildShardRanges, replicaCount);

        // Assert
        assertNotNull("Result should not be null", result);
        assertEquals("Shard ID should match parent", parentShardId, result.shardId());
        assertEquals("Current node ID should match parent", currentNodeId, result.currentNodeId());
        assertNull("Relocating node ID should be null", result.relocatingNodeId());
        assertEquals("Primary flag should match parent", primary, result.primary());
        assertEquals("Search only flag should match parent", searchOnly, result.isSearchOnly());
        assertEquals("State should be SPLITTING", ShardRoutingState.SPLITTING, result.state());
        assertNull("Unassigned info should be null", result.unassignedInfo());
        assertNotNull("Allocation ID should not be null", result.allocationId());
        assertEquals("Expected shard size should be unavailable", UNAVAILABLE_EXPECTED_SHARD_SIZE, result.getExpectedShardSize());

        ShardRouting[] childShards = result.getRecoveringChildShards();
        assertNotNull("Child shards should not be null", childShards);
        assertEquals("Number of child shards should match expected", recoveringChildShardRanges.length * (replicaCount + 1), childShards.length);

        for (int i = 0; i < recoveringChildShardRanges.length; i++) {
            ShardRouting primaryChildShard = childShards[i * (replicaCount + 1)];
            assertEquals("Child shard index should match", recoveringChildShardRanges[i].getShardId(), primaryChildShard.id());
            assertTrue("Primary child shard should be primary", primaryChildShard.primary());
            assertEquals("Primary child shard should be UNASSIGNED", ShardRoutingState.UNASSIGNED, primaryChildShard.state());
            assertEquals("Primary child shard should have correct recovery source",
                RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE, primaryChildShard.recoverySource());

            for (int j = 1; j <= replicaCount; j++) {
                ShardRouting replicaChildShard = childShards[i * (replicaCount + 1) + j];
                assertEquals("Replica child shard index should match", recoveringChildShardRanges[i].getShardId(), replicaChildShard.id());
                assertFalse("Replica child shard should not be primary", replicaChildShard.primary());
                assertEquals("Replica child shard should be UNASSIGNED", ShardRoutingState.UNASSIGNED, replicaChildShard.state());
                assertEquals("Replica child shard should have peer recovery source",
                    RecoverySource.PeerRecoverySource.INSTANCE, replicaChildShard.recoverySource());
            }
        }
    }

    public void testCreateRecoveringChildShardsWithInvalidParentState() {
        ShardRouting parentShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.INITIALIZING);
        ShardRange[] ranges = new ShardRange[]{ mock(ShardRange.class) };

        // Act & Assert
        AssertionError error = assertThrows(
            AssertionError.class,
            () -> parentShard.createRecoveringChildShards(ranges, 1)
        );

        assertEquals(
            "recovery source only available on unassigned or initializing shard but was SPLITTING",
            error.getMessage()
        );
    }

    public void testCreateRecoveringChildShardsWithNullShardRange() {
        ShardRouting parentShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        ShardRange[] ranges = new ShardRange[]{ null };

        assertThrows(NullPointerException.class, () -> {
            parentShard.createRecoveringChildShards(ranges, 1);
        });

    }

    /**
     * Test case for moveChildReplicaToStarted method
     *
     * This test verifies that the moveChildReplicaToStarted method correctly
     * creates a new ShardRouting with the expected properties for a child replica
     * shard that is being moved to the STARTED state.
     */
    public void testMoveChildReplicaToStarted() {
        // Arrange
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        String currentNodeId = "node1";
        boolean primary = false;
        boolean searchOnly = false;
        ShardRoutingState initialState = ShardRoutingState.INITIALIZING;
        RecoverySource recoverySource = RecoverySource.PeerRecoverySource.INSTANCE;
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test");
        AllocationId allocationId = AllocationId.newInitializing();
        long expectedShardSize = 1000L;
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);

        ShardRouting childReplica = new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            searchOnly,
            initialState,
            recoverySource,
            unassignedInfo,
            allocationId,
            expectedShardSize,
            null,
            parentShardId
        );

        // Act
        ShardRouting result = childReplica.moveChildReplicaToStarted();

        // Assert
        assertNotNull("Result should not be null", result);
        assertEquals("Shard ID should match", shardId, result.shardId());
        assertEquals("Current node ID should match", currentNodeId, result.currentNodeId());
        assertNull("Relocating node ID should be null", result.relocatingNodeId());
        assertEquals("Primary flag should match", primary, result.primary());
        assertEquals("Search only flag should match", searchOnly, result.isSearchOnly());
        assertEquals("State should be STARTED", ShardRoutingState.STARTED, result.state());
        assertNull("Recovery source should be null", result.recoverySource());
        assertNull("Unassigned info should be null", result.unassignedInfo());
        assertEquals("Allocation ID should match", allocationId, result.allocationId());
        assertEquals("Expected shard size should be unavailable", UNAVAILABLE_EXPECTED_SHARD_SIZE, result.getExpectedShardSize());
        assertNull("Recovering child shards should be null", result.getRecoveringChildShards());
        assertEquals("Parent shard ID should match", parentShardId, result.getParentShardId());
    }

    public void testMoveChildReplicaToStartedWithAlreadyStartedShard() {
        ShardRouting startedShard = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.STARTED);
        startedShard = new ShardRouting(startedShard.shardId(), startedShard.currentNodeId(), null, startedShard.primary(),
            startedShard.isSearchOnly(), startedShard.state(), null, null, startedShard.allocationId(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE, null, new ShardId("test", "_na_", 1));

        ShardRouting result = startedShard.moveChildReplicaToStarted();

        assertEquals("Shard should remain in STARTED state", ShardRoutingState.STARTED, result.state());
        assertEquals("Parent shard ID should not change", startedShard.getParentShardId(), result.getParentShardId());
    }

    public void testMoveToStartedFromNonInitializingState() {
        ShardRouting unassignedShard = TestShardRouting.newShardRouting("test", 0, null, true, ShardRoutingState.UNASSIGNED);
        assertThrows(AssertionError.class, unassignedShard::moveToStarted);

        ShardRouting startedShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertThrows(AssertionError.class, startedShard::moveToStarted);

        ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 0, "node1", "node2", true, ShardRoutingState.RELOCATING);
        assertThrows(AssertionError.class, relocatingShard::moveToStarted);
    }

    public void testMoveToStartedRetainsShardProperties() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.INITIALIZING);
        ShardRouting result = shard.moveToStarted();

        assertEquals(shard.shardId(), result.shardId());
        assertEquals(shard.currentNodeId(), result.currentNodeId());
        assertEquals(shard.primary(), result.primary());
        assertEquals(ShardRoutingState.STARTED, result.state());
        assertTrue(result.active());
        assertNull(result.relocatingNodeId());
        assertNull(result.recoverySource());
        assertNull(result.unassignedInfo());
    }

    public void testMoveToStartedWithParentAllocationId() {
        AllocationId parentAllocationId = AllocationId.newInitializing();
        AllocationId childAllocationId = AllocationId.newInitializing(parentAllocationId.getId());
        ShardRouting shard = new ShardRouting(
            new ShardId("test", "_na_", 0),
            "node1",
            null,
            true,
            false,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            null,
            childAllocationId,
            0,
            null,
            new ShardId("test", "_na_", 1)
        );

        ShardRouting result = shard.moveToStarted();
        assertNotNull(result);
        assertEquals(ShardRoutingState.STARTED, result.state());
        assertEquals(childAllocationId.getId(), result.allocationId().getId());
        assertNull(result.allocationId().getParentAllocationId());
    }

    public void testMoveToStartedWithValidRelocationId() {
        AllocationId allocationId = AllocationId.newRelocation(AllocationId.newInitializing());
        ShardRouting shard = new ShardRouting(
            new ShardId("test", "_na_", 0),
            "node1",
            "node2",
            true,
            false,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            null,
            allocationId,
            0,
            null,
            null
        );

        ShardRouting result = shard.moveToStarted();
        assertNotNull(result);
        assertEquals(ShardRoutingState.STARTED, result.state());
        assertEquals(allocationId.getId(), result.allocationId().getId());
    }

    /**
     * Test the correct behavior of removeParentFromReplica
     */
    public void testRemoveParentFromReplicaCorrectBehavior() {
        ShardId parentShardId = new ShardId("test", "_na_", 0);
        ShardId childShardId = new ShardId("test", "_na_", 1);

        ShardRouting childReplica = new ShardRouting(
            childShardId,
            "node1",
            null,
            false,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        ShardRouting result = childReplica.removeParentFromReplica();

        assertNotNull("Result should not be null", result);
        assertEquals("Shard ID should remain unchanged", childShardId, result.shardId());
        assertEquals("Node ID should remain unchanged", "node1", result.currentNodeId());
        assertNull("Relocating node ID should be null", result.relocatingNodeId());
        assertFalse("Should not be primary", result.primary());
        assertFalse("Should not be search only", result.isSearchOnly());
        assertEquals("State should be STARTED", ShardRoutingState.STARTED, result.state());
        assertNull("Recovery source should be null", result.recoverySource());
        assertNull("Unassigned info should be null", result.unassignedInfo());
        assertEquals("Allocation ID should remain unchanged", childReplica.allocationId(), result.allocationId());
        assertEquals("Expected shard size should be unavailable", UNAVAILABLE_EXPECTED_SHARD_SIZE, result.getExpectedShardSize());
        assertNull("Recovering child shards should be null", result.getRecoveringChildShards());
        assertNull("Parent shard ID should be null", result.getParentShardId());
    }

    /**
     * Negative test cases for `removeParentFromReplica`
     */
    public void testRemoveParentFromReplicaNegativeCases() {
        // Test case 1: Shard is not a started child replica
        ShardRouting nonStartedChildReplica = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.INITIALIZING);
        assertThrows(AssertionError.class, nonStartedChildReplica::removeParentFromReplica);

        // Test case 2: Shard is a primary
        ShardRouting primaryShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertThrows(AssertionError.class, primaryShard::removeParentFromReplica);

        // Test case 3: Shard is not assigned to a node
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        assertThrows(AssertionError.class, unassignedShard::removeParentFromReplica);

        // Test case 4: Shard is not in STARTED state
        ShardRouting initializingShard = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.INITIALIZING);
        assertThrows(AssertionError.class, initializingShard::removeParentFromReplica);

        // Test case 5: Shard has no parent
        ShardRouting noParentShard = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.STARTED);
        assertThrows(AssertionError.class, noParentShard::removeParentFromReplica);
    }


    /**
     * Negative test cases for `getParentShardId`
     */
    public void testGetParentShardIdNegativeCases() {
        // Test case 1: Shard with no parent
        ShardRouting shardWithNoParent = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertNull("Shard with no parent should return null", shardWithNoParent.getParentShardId());

        // Test case 2: Unassigned shard
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        assertNull("Unassigned shard should return null", unassignedShard.getParentShardId());

        // Test case 3: Initializing shard without parent
        ShardRouting initializingShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.INITIALIZING);
        assertNull("Initializing shard without parent should return null", initializingShard.getParentShardId());

        // Test case 4: Relocating shard
        ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 0, "node1", "node2", true, ShardRoutingState.RELOCATING);
        assertNull("Relocating shard should return null", relocatingShard.getParentShardId());
    }

    /**
     * Negative test cases for `getRecoveringChildShards`
     */
    public void testGetRecoveringChildShardsNegativeCases() {
        // Test case 1: Shard with no recovering child shards
        ShardRouting shardWithNoChildren = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertNull("Should return null for shard with no recovering child shards", shardWithNoChildren.getRecoveringChildShards());

        // Test case 2: Unassigned shard
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        assertNull("Should return null for unassigned shard", unassignedShard.getRecoveringChildShards());

        // Test case 3: Initializing shard
        ShardRouting initializingShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.INITIALIZING);
        assertNull("Should return null for initializing shard", initializingShard.getRecoveringChildShards());

        // Test case 4: Relocating shard
        ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 0, "node1", "node2", true, ShardRoutingState.RELOCATING);
        assertNull("Should return null for relocating shard", relocatingShard.getRecoveringChildShards());

        // Test case 5: Non-primary shard
        ShardRouting replicaShard = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.STARTED);
        assertNull("Should return null for non-primary shard", replicaShard.getRecoveringChildShards());

    }

    /**
     * Test case for getParentShardId method
     *
     * This test verifies that the getParentShardId method correctly returns the parent shard ID
     * when it is set, and returns null when it is not set.
     */
    public void test_getParentShardId_1() {
        // Arrange
        ShardId childShardId = new ShardId("test_index", "_na_", 1);
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);

        // Create a shard routing with a parent shard ID
        ShardRouting shardWithParent = new ShardRouting(
            childShardId,
            "node1",
            null,
            false,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // Create a shard routing without a parent shard ID
        ShardRouting shardWithoutParent = new ShardRouting(
            childShardId,
            "node1",
            null,
            false,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );

        // Act & Assert
        assertEquals("Parent shard ID should match the one set", parentShardId, shardWithParent.getParentShardId());
        assertNull("Parent shard ID should be null when not set", shardWithoutParent.getParentShardId());
    }

    /**
    * Test case for getRecoveringChildShards method
    *
    * This test verifies that the getRecoveringChildShards method correctly returns
    * the recovering child shards without creating a defensive copy.
    */
    public void test_getRecoveringChildShards_1() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        String currentNodeId = "node1";
        boolean primary = true;
        ShardRoutingState state = ShardRoutingState.SPLITTING;
        AllocationId allocationId = AllocationId.newInitializing();

        ShardRouting[] childShards = new ShardRouting[] {
            TestShardRouting.newShardRouting(new ShardId("test_index", "_na_", 1), null, true, ShardRoutingState.UNASSIGNED),
            TestShardRouting.newShardRouting(new ShardId("test_index", "_na_", 2), null, false, ShardRoutingState.UNASSIGNED)
        };

        ShardRouting parentShard = new ShardRouting(
            parentShardId,
            currentNodeId,
            null,
            primary,
            false,
            state,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            childShards,
            null
        );

        // Act
        ShardRouting[] result = parentShard.getRecoveringChildShards();

        // Assert
        assertNotNull("Returned array should not be null", result);
        assertSame("Returned array should be the same instance as the original", childShards, result);
        assertEquals("Returned array should have the correct number of child shards", 2, result.length);

        // Verify that modifying the returned array affects the original
        result[0] = null;
        assertNull("Modifying the returned array should affect the original", childShards[0]);
    }

    /**
    * Test case for public boolean isStartedChildReplica()
    *
    * This test verifies that isStartedChildReplica() returns true when:
    * - The shard is not primary (primary == false)
    * - The shard has a parent shard ID (getParentShardId() != null)
    * - The shard is in started state (started() == true)
    */
    public void test_isStartedChildReplica_1() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);
        String nodeId = "node1";
        boolean primary = false;
        boolean searchOnly = false;
        ShardRoutingState state = ShardRoutingState.STARTED;
        AllocationId allocationId = AllocationId.newInitializing();

        ShardRouting childReplica = new ShardRouting(
            childShardId,
            nodeId,
            null,
            primary,
            searchOnly,
            state,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // Act
        boolean result = childReplica.isStartedChildReplica();

        // Assert
        assertTrue("The shard should be identified as a started child replica", result);
        assertFalse("The shard should not be primary", childReplica.primary());
        assertNotNull("The shard should have a parent shard ID", childReplica.getParentShardId());
        assertTrue("The shard should be in started state", childReplica.started());
    }

    /**
    * Negative test case for public boolean isStartedChildReplica()
    *
    * This test verifies that isStartedChildReplica() returns false when any of the conditions are not met:
    * - The shard is primary
    * - The shard doesn't have a parent shard ID
    * - The shard is not in started state
    */
    public void test_isStartedChildReplica_negative() {
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        String nodeId = "node1";
        AllocationId allocationId = AllocationId.newInitializing();

        // Case 1: Primary shard
        ShardRouting primaryShard = new ShardRouting(
            shardId,
            nodeId,
            null,
            true,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            new ShardId("test_index", "_na_", 0)
        );
        assertFalse("Primary shard should not be identified as a started child replica", primaryShard.isStartedChildReplica());

        // Case 2: No parent shard ID
        ShardRouting noParentShard = new ShardRouting(
            shardId,
            nodeId,
            null,
            false,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );
        assertFalse("Shard without parent should not be identified as a started child replica", noParentShard.isStartedChildReplica());

        // Case 3: Not in started state
        ShardRouting notStartedShard = new ShardRouting(
            shardId,
            nodeId,
            null,
            true,
            false,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            null,
            allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            new ShardId("test_index", "_na_", 0)
        );
        assertFalse("Non-started shard should not be identified as a started child replica", notStartedShard.isStartedChildReplica());
    }

    /**
     * Negative test cases for `isStartedChildReplica`
     */
    public void test_isStartedChildReplica_negative_cases() {
        // Test case 1: Primary shard
        ShardRouting primaryShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Primary shard should not be a started child replica", primaryShard.isStartedChildReplica());

        // Test case 2: Unassigned shard
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        assertFalse("Unassigned shard should not be a started child replica", unassignedShard.isStartedChildReplica());

        // Test case 3: Initializing shard
        ShardRouting initializingShard = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.INITIALIZING);
        assertFalse("Initializing shard should not be a started child replica", initializingShard.isStartedChildReplica());

        // Test case 4: Relocating shard
        ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 0, "node1", "node2", false, ShardRoutingState.RELOCATING);
        assertFalse("Relocating shard should not be a started child replica", relocatingShard.isStartedChildReplica());

        // Test case 5: Started shard without parent
        ShardRouting startedShardWithoutParent = TestShardRouting.newShardRouting("test", 0, "node1", false, ShardRoutingState.STARTED);
        assertFalse("Started shard without parent should not be a started child replica", startedShardWithoutParent.isStartedChildReplica());

        // Test case 6: Started primary child shard
        ShardId parentShardId = new ShardId("test", "_na_", 0);
        ShardRouting startedPrimaryChildShard = new ShardRouting(
            new ShardId("test", "_na_", 1),
            "node1",
            null,
            true,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Started primary child shard should not be a started child replica", startedPrimaryChildShard.isStartedChildReplica());

    }

    /**
    * Test case for unassignedReasonChildShardCreated method
    * Path constraints: (unassignedInfo != null)
    * Expected result: unassignedInfo.getReason() == UnassignedInfo.Reason.CHILD_SHARD_CREATED
    */
    public void test_unassignedReasonChildShardCreated_1() {
        // Arrange
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CHILD_SHARD_CREATED, "Test reason");
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            unassignedInfo
        );

        // Act
        boolean result = shardRouting.unassignedReasonChildShardCreated();

        // Assert
        assertTrue("Expected unassignedReasonChildShardCreated to return true for CHILD_SHARD_CREATED reason", result);
    }

    /**
    * Test case for unassignedReasonChildShardCreated method
    * Path constraints: (unassignedInfo != null)
    * Expected result: unassignedInfo.getReason() != UnassignedInfo.Reason.CHILD_SHARD_CREATED
    */
    public void test_unassignedReasonChildShardCreated_2() {
        // Arrange
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Test reason");
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            unassignedInfo
        );

        // Act
        boolean result = shardRouting.unassignedReasonChildShardCreated();

        // Assert
        assertFalse("Expected unassignedReasonChildShardCreated to return false for non-CHILD_SHARD_CREATED reason", result);
    }

    /**
    * Test case for unassignedReasonChildShardCreated method
    * Path constraints: (unassignedInfo == null)
    * Expected result: false
    */
    public void test_unassignedReasonChildShardCreated_3() {
        // Arrange
        ShardId shardId = new ShardId("test_index", "_na_", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);

        // Act
        boolean result = shardRouting.unassignedReasonChildShardCreated();

        // Assert
        assertFalse("Expected unassignedReasonChildShardCreated to return false when unassignedInfo is null", result);
    }

    /**
     * Negative test cases for `unassignedReasonChildShardCreated`
     */
    public void test_unassignedReasonChildShardCreated_negative_tests() {
        // Test when unassignedInfo is null
        ShardRouting shardWithNullUnassignedInfo = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Should return false when unassignedInfo is null", shardWithNullUnassignedInfo.unassignedReasonChildShardCreated());

        // Test when shard is not in UNASSIGNED state
        ShardRouting assignedShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Should return false when shard is not UNASSIGNED", assignedShard.unassignedReasonChildShardCreated());

        // Test when unassigned reason is not CHILD_SHARD_CREATED
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test reason");
        ShardRouting shardWithDifferentReason = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            unassignedInfo
        );
        assertFalse("Should return false when reason is not CHILD_SHARD_CREATED", shardWithDifferentReason.unassignedReasonChildShardCreated());

    }

    /**
     * Positive test case for `unassignedReasonChildShardCreated`
     */
    public void test_unassignedReasonChildShardCreated_positive_test() {
        UnassignedInfo childShardCreatedInfo = new UnassignedInfo(UnassignedInfo.Reason.CHILD_SHARD_CREATED, "child shard created");
        ShardRouting childShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 1),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            childShardCreatedInfo
        );
        assertTrue("Should return true for CHILD_SHARD_CREATED reason", childShard.unassignedReasonChildShardCreated());
    }

    /**
     * Test case for isSplitTargetOf method
     * This test verifies that a shard is correctly identified as a split target of another shard
     */
    public void testIsSplitTargetOf() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);

        AllocationId parentAllocationId = AllocationId.newInitializing();
        parentAllocationId = AllocationId.newSplit(parentAllocationId, 2);
        String firstChildId = parentAllocationId.getSplitChildAllocationIds().iterator().next();
        AllocationId childAllocationId = AllocationId.newInitializing(firstChildId);
        childAllocationId = AllocationId.newTargetSplit(parentAllocationId, childAllocationId.getId());

        ShardRouting parentShard = new ShardRouting(
            parentShardId,
            "node1",
            null,
            true,
            false,
            ShardRoutingState.SPLITTING,
            null,
            null,
            parentAllocationId,
            0,
            null,
            null
        );

        ShardRouting childShard = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            childAllocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // Act
        boolean result = childShard.isSplitTargetOf(parentShard);

        // Assert
        assertTrue("Child shard should be identified as split target of parent shard", result);
        assertEquals("Parent shard should be in SPLITTING state", ShardRoutingState.SPLITTING, parentShard.state());
        assertTrue("Parent shard should be primary", parentShard.primary());
        assertNotNull("Parent shard's split child allocation IDs should not be null", parentShard.allocationId().getSplitChildAllocationIds());
        assertTrue("Parent shard's split child allocation IDs should contain child's allocation ID",
            parentShard.allocationId().getSplitChildAllocationIds().contains(childShard.allocationId().getId()));
        assertEquals("Child shard's parent shard ID should match parent's shard ID", parentShardId, childShard.getParentShardId());
    }

    /**
     * Negative test cases for `isSplitTargetOf`
     */
    public void test_isSplitTargetOf_negative_cases() {
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);

        // Setup a parent shard
        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.SPLITTING);
        parentShard = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            null,
            parentShard.primary(),
            parentShard.isSearchOnly(),
            parentShard.state(),
            null,
            null,
            AllocationId.newSplit(AllocationId.newInitializing(), 2),
            0,
            null,
            null
        );

        // Setup a child shard
        ShardRouting childShard = TestShardRouting.newShardRouting(childShardId, null, false, ShardRoutingState.UNASSIGNED);
        childShard = new ShardRouting(
            childShardId,
            null,
            null,
            childShard.primary(),
            childShard.isSearchOnly(),
            childShard.state(),
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // Test case 2: Parent shard is not in SPLITTING state
        ShardRouting nonSplittingParent = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Should return false when parent is not in SPLITTING state", childShard.isSplitTargetOf(nonSplittingParent));

        // Test case 3: Child shard has no parent allocation ID
        ShardRouting childWithNoParent = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when child has no parent allocation ID", childWithNoParent.isSplitTargetOf(parentShard));

        // Test case 4: Parent shard has no allocation ID
        ShardRouting parentWithNoAllocationId = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.SPLITTING);
        assertFalse("Should return false when parent has no allocation ID", childShard.isSplitTargetOf(parentWithNoAllocationId));

        // Test case 5: Parent and child allocation IDs don't match
        ShardRouting childWithDifferentParent = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing("different_parent_id"),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when parent and child allocation IDs don't match", childWithDifferentParent.isSplitTargetOf(parentShard));

        // Test case 6: Child shard ID doesn't match parent's split child IDs
        ShardId differentChildShardId = new ShardId("test_index", "_na_", 2);
        ShardRouting differentChildShard = new ShardRouting(
            differentChildShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when child shard ID doesn't match parent's split child IDs", differentChildShard.isSplitTargetOf(parentShard));

        // Test case 7: Child's parent shard ID doesn't match parent's shard ID
        ShardId differentParentShardId = new ShardId("test_index", "_na_", 3);
        ShardRouting childWithDifferentParentId = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            differentParentShardId
        );
        assertFalse("Should return false when child's parent shard ID doesn't match parent's shard ID", childWithDifferentParentId.isSplitTargetOf(parentShard));

        // Test case 8: Parent is not primary
        ShardRouting nonPrimaryParent = TestShardRouting.newShardRouting(parentShardId, "node1", false, ShardRoutingState.SPLITTING);
        assertFalse("Should return false when parent is not primary", childShard.isSplitTargetOf(nonPrimaryParent));
    }

    /**
     * Test case for isSplitTarget method
     *
     * This test verifies that isSplitTarget returns true when the shard has a parent shard ID,
     * and false when it doesn't.
     */
    public void test_isSplitTarget() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);

        // Create a shard routing with a parent shard ID (split target)
        ShardRouting splitTargetShard = new ShardRouting(
            childShardId,
            "node1",
            null,
            false,
            false,
            ShardRoutingState.INITIALIZING,
            RecoverySource.PeerRecoverySource.INSTANCE,
            null,
            AllocationId.newInitializing(),
            0,
            null,
            parentShardId
        );

        // Create a shard routing without a parent shard ID (not a split target)
        ShardRouting nonSplitTargetShard = new ShardRouting(
            childShardId,
            "node1",
            null,
            false,
            false,
            ShardRoutingState.STARTED,
            null,
            null,
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            null
        );

        // Act & Assert
        assertTrue("Shard with parent ID should be identified as split target", splitTargetShard.isSplitTarget());
        assertFalse("Shard without parent ID should not be identified as split target", nonSplitTargetShard.isSplitTarget());
    }

    /**
    * Negative test cases for `isSplitTarget`
    * Generate a test for each of the following scenarios relevant for `isSplitTarget`:
    * 1. input is empty and/or invalid;
    * 2. input is outside accepted bounds;
    * 3. input is incorrect type and/or input is incorrect format;
    * 4. exceptions are tested;
    * 5. other edge cases for `isSplitTarget` are tested
    */
    public void test_isSplitTarget_negative_cases() {
        // 1. Test with null ShardRouting
        assertThrows(NullPointerException.class, () -> {
            ShardRouting nullShard = null;
            nullShard.isSplitTarget();
        });

        // 2. Test with unassigned shard (outside accepted bounds)
        ShardRouting unassignedShard = ShardRouting.newUnassigned(
            new ShardId("test", "_na_", 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );
        assertFalse("Unassigned shard should not be a split target", unassignedShard.isSplitTarget());

        // 3. Test with incorrect state (not INITIALIZING)
        ShardRouting startedShard = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Started shard should not be a split target", startedShard.isSplitTarget());

        // 4. Test exception handling (no exception should be thrown, but method should return false)
        ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 0, "node1", "node2", true, ShardRoutingState.RELOCATING);
        assertFalse("Relocating shard should not be a split target", relocatingShard.isSplitTarget());

        // 5. Edge case: Initializing shard without parent
        ShardRouting initializingShardNoParent = TestShardRouting.newShardRouting("test", 0, "node1", true, ShardRoutingState.INITIALIZING);
        assertFalse("Initializing shard without parent should not be a split target", initializingShardNoParent.isSplitTarget());
    }

    /**
     * Test case for isSplitSourceOf method
     * This test verifies that a shard is correctly identified as a split source of another shard
     */
    public void testIsSplitSourceOf() {
        // Arrange
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);

        AllocationId parentAllocationId = AllocationId.newInitializing();
        parentAllocationId = AllocationId.newSplit(parentAllocationId, 2);
        String firstChildId = parentAllocationId.getSplitChildAllocationIds().iterator().next();
        AllocationId childAllocationId = AllocationId.newInitializing(firstChildId);
        childAllocationId = AllocationId.newTargetSplit(parentAllocationId, childAllocationId.getId());

        ShardRouting parentShard = new ShardRouting(
            parentShardId,
            "node1",
            null,
            true,
            false,
            ShardRoutingState.SPLITTING,
            null,
            null,
            parentAllocationId,
            0,
            null,
            null
        );

        ShardRouting childShard = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            childAllocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // Act
        boolean result = parentShard.isSplitSourceOf(childShard);

        // Assert
        assertTrue("Parent shard should be identified as split source of child shard", result);
        assertEquals("Parent shard should be in SPLITTING state", ShardRoutingState.SPLITTING, parentShard.state());
        assertTrue("Parent shard should be primary", parentShard.primary());
        assertNotNull("Parent shard's split child allocation IDs should not be null", parentShard.allocationId().getSplitChildAllocationIds());
        assertTrue("Parent shard's split child allocation IDs should contain child's allocation ID", parentShard.allocationId().getSplitChildAllocationIds().contains(childShard.allocationId().getId()));
        assertEquals("Child shard's parent shard ID should match parent's shard ID", parentShardId, childShard.getParentShardId());
    }

    /**
     * Negative test cases for `isSplitSourceOf`
     */
    public void test_isSplitSourceOf_negative_cases() {
        ShardId parentShardId = new ShardId("test_index", "_na_", 0);
        ShardId childShardId = new ShardId("test_index", "_na_", 1);

        // Setup a parent shard
        ShardRouting parentShard = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.SPLITTING);
        parentShard = new ShardRouting(
            parentShardId,
            parentShard.currentNodeId(),
            null,
            parentShard.primary(),
            parentShard.isSearchOnly(),
            parentShard.state(),
            null,
            null,
            AllocationId.newSplit(AllocationId.newInitializing(), 2),
            0,
            null,
            null
        );

        // Setup a child shard
        ShardRouting childShard = TestShardRouting.newShardRouting(childShardId, null, false, ShardRoutingState.UNASSIGNED);
        childShard = new ShardRouting(
            childShardId,
            null,
            null,
            childShard.primary(),
            childShard.isSearchOnly(),
            childShard.state(),
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );

        // 2. Test when parent shard is not in SPLITTING state
        ShardRouting nonSplittingParent = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.STARTED);
        assertFalse("Should return false when parent is not in SPLITTING state", nonSplittingParent.isSplitSourceOf(childShard));

        // 3. Test when child shard has no parent allocation ID
        ShardRouting childWithNoParent = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when child has no parent allocation ID", parentShard.isSplitSourceOf(childWithNoParent));

        // 4. Test when parent shard has no allocation ID
        ShardRouting parentWithNoAllocationId = TestShardRouting.newShardRouting(parentShardId, "node1", true, ShardRoutingState.SPLITTING);
        assertFalse("Should return false when parent has no allocation ID", parentWithNoAllocationId.isSplitSourceOf(childShard));

        // 5. Test when parent and child allocation IDs don't match
        ShardRouting childWithDifferentParent = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing("different_parent_id"),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when parent and child allocation IDs don't match", parentShard.isSplitSourceOf(childWithDifferentParent));

        // 6. Test when child shard ID doesn't match parent's split child IDs
        ShardId differentChildShardId = new ShardId("test_index", "_na_", 2);
        ShardRouting differentChildShard = new ShardRouting(
            differentChildShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            parentShardId
        );
        assertFalse("Should return false when child shard ID doesn't match parent's split child IDs", parentShard.isSplitSourceOf(differentChildShard));

        // 7. Test when child's parent shard ID doesn't match parent's shard ID
        ShardId differentParentShardId = new ShardId("test_index", "_na_", 3);
        ShardRouting childWithDifferentParentId = new ShardRouting(
            childShardId,
            null,
            null,
            false,
            false,
            ShardRoutingState.UNASSIGNED,
            RecoverySource.InPlaceShardSplitRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            AllocationId.newInitializing(parentShard.allocationId().getId()),
            UNAVAILABLE_EXPECTED_SHARD_SIZE,
            null,
            differentParentShardId
        );
        assertFalse("Should return false when child's parent shard ID doesn't match parent's shard ID", parentShard.isSplitSourceOf(childWithDifferentParentId));

        // 8. Test when parent is not primary
        ShardRouting nonPrimaryParent = TestShardRouting.newShardRouting(parentShardId, "node1", false, ShardRoutingState.SPLITTING);
        assertFalse("Should return false when parent is not primary", nonPrimaryParent.isSplitSourceOf(childShard));
    }
}
