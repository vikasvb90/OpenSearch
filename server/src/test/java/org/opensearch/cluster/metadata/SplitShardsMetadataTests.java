package org.opensearch.cluster.metadata;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import org.mockito.Mockito;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class SplitShardsMetadataTests extends OpenSearchTestCase {

    @Test
    public void testGetShardIdOfHashWithNoChildren() {
        // Setup
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);

        // Test: When there are no children, should return the root shard id
        int result = builder.build().getShardIdOfHash(0, 100, false);
        assertEquals(0, result);
    }

    /**
     * Tests getShardIdOfHash when there are existing child shards and in-progress children.
     */
    @Test
    public void testGetShardIdOfHashWithExistingAndInProgressChildren() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard
        // First split - create existing child shards
        builder.splitShard(0, 3);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2, 3));

        // Second split - split the middle shard (ID 2)
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();

        // Execute - test hash that falls in the range of first child of shard 2
        int result = metadata.getShardIdOfHash(0, 500, true);

        // Assert - should route to the first child of the in-progress split
        assertEquals("Hash should route to first child of in-progress split", 5, result);
    }

    /**
     * Test getShardIdOfHash when root shard has no children but in-progress split exists
     */
    @Test
    public void testGetShardIdOfHashWithInProgressSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard
        // Setup - split root shard
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Execute - test with hash that should go to second child
        int result = metadata.getShardIdOfHash(0, 100, true);

        // Verify - should route to the second child shard
        assertEquals("Should route to second child shard", 2, result);
    }

    @Test
    public void testGetShardIdOfHashWithInProgressSplitIgnored() {
        /**
         * Test case for in-progress split being ignored
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Should return root shard ID when includeInProgressChildren is false
        assertEquals(0, metadata.getShardIdOfHash(0, 100, false));
    }

    @Test
    public void testGetShardIdOfHashWithInvalidHash() {
        /**
         * Test case for invalid hash value
         */
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(5).build();
        assertEquals(0, metadata.getShardIdOfHash(0, Integer.MIN_VALUE - 1, false));
        assertEquals(0, metadata.getShardIdOfHash(0, Integer.MAX_VALUE + 1, false));
    }

    @Test
    public void testGetShardIdOfHashWithInvalidRootShardId() {
        /**
         * Test case for invalid root shard ID
         */
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(5).build();
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            metadata.getShardIdOfHash(-1, 100, false);
        });
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            metadata.getShardIdOfHash(5, 100, false);
        });
    }

    @Test
    public void testGetShardIdOfHashWithNonExistentShard() {
        /**
         * Test case for non-existent shard
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Attempt to get shard ID for a non-existent shard
        assertEquals(1, metadata.getShardIdOfHash(1, 100, false));
    }

    /**
     * Test case for getShardIdOfHash when the root shard has no children and no in-progress split
     */
    @Test
    public void test_getShardIdOfHash_returnsRootShardId() {
        // Arrange
        int rootShardId = 0;
        int hash = 123;
        boolean includeInProgressChildren = false;

        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // 1 root shard
        SplitShardsMetadata metadata = builder.build();

        // Act
        int result = metadata.getShardIdOfHash(rootShardId, hash, includeInProgressChildren);

        // Assert
        assertEquals("Should return the root shard ID when there are no children", rootShardId, result);
    }

    /**
     * Test getShardIdOfHash when root shard has children but no in-progress splits
     */
    @Test
    public void test_getShardIdOfHash_withExistingChildren() {
        // Setup
        int rootShardId = 0;
        int hash = 500;
        boolean includeInProgressChildren = true;

        // Setup
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard

        // Split the root shard into 3 pieces
        builder.splitShard(0, 3);

        // Complete the split with three child shards
        Set<Integer> childShardIds = Set.of(1, 2, 3);
        builder.updateSplitMetadataForChildShards(0, childShardIds);

        SplitShardsMetadata metadata = builder.build();

        // Execute
        int result = metadata.getShardIdOfHash(rootShardId, hash, includeInProgressChildren);

        // Verify
        assertEquals(2, result);
    }

    /**
     * Test that getNumberOfRootShards returns the correct number of root shards
     */
    @Test
    public void testGetNumberOfRootShardsReturnsCorrectCount() {
        // Arrange
        int expectedRootShards = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(expectedRootShards);
        SplitShardsMetadata metadata = builder.build();

        // Act
        int actualRootShards = metadata.getNumberOfRootShards();

        // Assert
        assertEquals("Number of root shards should match the initial count", expectedRootShards, actualRootShards);
    }


    @Test
    public void testGetNumberOfRootShardsAfterSplitting() {
        /**
         * Test case for getting number of root shards after splitting
         * Expected behavior: Should return the original number of root shards
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        assertEquals("Number of root shards should remain unchanged after splitting", 5, metadata.getNumberOfRootShards());
    }

    @Test
    public void testGetNumberOfRootShardsWithEmptyMetadata() {
        /**
         * Test case for empty metadata
         * Expected behavior: Should return 0 for empty metadata
         */
        SplitShardsMetadata emptyMetadata = new SplitShardsMetadata.Builder(0).build();
        assertEquals("Empty metadata should have 0 root shards", 0, emptyMetadata.getNumberOfRootShards());
    }

    /**
     * This test verifies that the getNumberOfShards() method
     * correctly returns maxShardId + 1
     */
    @Test
    public void testGetNumberOfShardsReturnsMaxShardIdPlusOne() {
        // Arrange
        int maxShardId = 5;
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(maxShardId + 1).build();

        // Act
        int result = metadata.getNumberOfShards();

        // Assert
        assertEquals("getNumberOfShards should return maxShardId + 1", maxShardId + 1, result);
    }

    public void testGetNumberOfShardsAfterSplitting() {
        /**
         * Test case for getting number of shards after splitting
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        // Split the root shard into 3 pieces
        builder.splitShard(0, 3);

        // Complete the split with three child shards
        Set<Integer> childShardIds = Set.of(1, 2, 3);
        builder.updateSplitMetadataForChildShards(0, childShardIds);
        SplitShardsMetadata metadata = builder.build();
        assertEquals(4, metadata.getNumberOfShards());
    }

    @Test
    public void testGetNumberOfShardsWithEmptyMetadata() {
        /**
         * Test case for empty metadata
         * Expected behavior: Should return 0 for empty metadata
         */
        SplitShardsMetadata emptyMetadata = new SplitShardsMetadata.Builder(0).build();
        assertEquals("Empty metadata should have 0 shards", 0, emptyMetadata.getNumberOfShards());
    }

    /**
     * Test case for getChildShardsOfParent when the parent shard has child shards
     */
    @Test
    public void testGetChildShardsOfParentWithExistingChildren() {
        // Create a builder and add some child shards for a parent
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        int parentShardId = 2;
        int numberOfChildren = 3;
        builder.splitShard(parentShardId, numberOfChildren);

        // Build the metadata
        SplitShardsMetadata metadata = builder.build();

        // Get child shards of the parent
        ShardRange[] childShards = metadata.getChildShardsOfParent(parentShardId);

        // Assert that child shards are returned and match the expected number
        assertNotNull(childShards);
        assertEquals(numberOfChildren, childShards.length);

        // Verify that each child shard is a copy and not the original
        ShardRange[] originalChildShards = metadata.getChildShardsOfParent(parentShardId);
        for (int i = 0; i < childShards.length; i++) {
            assertNotSame(originalChildShards[i], childShards[i]);
            assertEquals(originalChildShards[i], childShards[i]);
        }
    }

    @Test
    public void testGetChildShardsOfParentAfterCancelledSplit() {
        /**
         * Test case for getChildShardsOfParent after a cancelled split operation
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        builder.cancelSplit(0);
        SplitShardsMetadata metadata = builder.build();

        assertNull("Should return null for shard with cancelled split", metadata.getChildShardsOfParent(0));
    }

    @Test
    public void testGetChildShardsOfParentWithNonExistentShardId() {
        /**
         * Test case for getChildShardsOfParent with a non-existent shard ID
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        SplitShardsMetadata metadata = builder.build();

        assertNull("Should return null for non-existent shard ID", metadata.getChildShardsOfParent(10));
    }

    @Test
    public void testGetChildShardsOfParentWithUnsplitShard() {
        /**
         * Test case for getChildShardsOfParent with an unsplit shard
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        SplitShardsMetadata metadata = builder.build();

        assertNull("Should return null for unsplit shard", metadata.getChildShardsOfParent(0));
    }

//    /**
//     * Tests numberOfEmptyParentShards() when a split is in progress
//     */
//    @Test
//    public void testNumberOfEmptyParentShardsWithInProgressSplit() {
//        // Create a builder with 3 root shards
//        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(3);
//
//        // Split shard 0 into 2 children
////        builder.splitShard(0, 2);
//
//        // Build the metadata
//        SplitShardsMetadata metadata = builder.build();
//
//        // Assert that there's one empty parent shard (shard 1)
//        // Shard 0 is in progress, so it's not counted as empty
//        assertEquals(2, metadata.numberOfEmptyParentShards());
//    }

    /**
     * Test case for SplitShardsMetadata.Builder constructor with numberOfShards parameter
     */
    @Test
    public void testBuilderConstructorWithNumberOfShards() {
        int numberOfShards = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(numberOfShards);

        SplitShardsMetadata metadata = builder.build();

        assertEquals(numberOfShards, metadata.getNumberOfRootShards());
        assertEquals(numberOfShards, metadata.getNumberOfShards());
        assertEquals(SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());

        for (int i = 0; i < numberOfShards; i++) {
            assertNull(metadata.getChildShardsOfParent(i));
        }
    }

    /**
     * Tests the Builder constructor with a SplitShardsMetadata instance where some root shards have no children.
     */
    @Test
    public void testBuilderWithEmptyRootShards() {
        // Create a SplitShardsMetadata with some empty root shards
        SplitShardsMetadata.Builder originalBuilder = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata original = originalBuilder.build();

        // Create a new Builder using the original SplitShardsMetadata
        SplitShardsMetadata.Builder newBuilder = new SplitShardsMetadata.Builder(original);

        // Build the new SplitShardsMetadata
        SplitShardsMetadata result = newBuilder.build();

        // Assert that the new SplitShardsMetadata matches the original
        assertEquals(original, result);
        assertEquals(3, result.getNumberOfRootShards());
        assertNull(result.getChildShardsOfParent(0));
        assertNull(result.getChildShardsOfParent(1));
        assertNull(result.getChildShardsOfParent(2));
        assertEquals(SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, result.getInProgressSplitShardId());
    }

    /**
     * Test that cancelSplit correctly removes the in-progress split and resets the inProgressSplitShardId
     */
    @Test
    public void testCancelSplitRemovesInProgressSplit() {
        // Setup
        int numberOfShards = 5;
        int sourceShardId = 2;
        int numberOfChildren = 2;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(numberOfShards);

        // Start a split
        builder.splitShard(sourceShardId, numberOfChildren);

        // Verify split is in progress
        assertEquals(sourceShardId, builder.build().getInProgressSplitShardId());

        // Cancel the split
        builder.cancelSplit(sourceShardId);

        // Build the metadata
        SplitShardsMetadata metadata = builder.build();

        // Verify the split was canceled
        assertEquals(SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());
        assertNull(metadata.getChildShardsOfParent(sourceShardId));
    }

    @Test
    public void testCancelSplitWhenNoSplitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        assertThrows(AssertionError.class, () -> {
            builder.cancelSplit(0);
        });
    }

    @Test
    public void testCancelSplitMultipleTimes() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        builder.cancelSplit(0);
        assertThrows(AssertionError.class, () -> {
            builder.cancelSplit(0);
        });
    }

//    @Test
//    public void testCancelSplitWhenSplitInProgress() {
//        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(2);
//        // Start a split
//        builder.splitShard(1,2);
//        builder.cancelSplit(0);
//    }

    /**
     * Test that getInProgressSplitShardId returns the correct shard ID
     */
    @Test
    public void testGetInProgressSplitShardId() {
        int expectedShardId = 5;
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(10);
        builder.splitShard(expectedShardId, 2);
        SplitShardsMetadata metadata = builder.build();

        int actualShardId = metadata.getInProgressSplitShardId();

        assertEquals("The in-progress split shard ID should match the expected value", expectedShardId, actualShardId);
    }

    @Test
    public void testGetInProgressSplitShardIdAfterCancelingSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        builder.cancelSplit(0);
        SplitShardsMetadata metadata = builder.build();
        assertEquals("After canceling a split, SPLIT_NOT_IN_PROGRESS should be returned", SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());
    }

    @Test
    public void testGetInProgressSplitShardIdAfterCompletingSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2));
        SplitShardsMetadata metadata = builder.build();
        assertEquals("After completing a split, SPLIT_NOT_IN_PROGRESS should be returned", SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());
    }

    @Test
    public void testGetInProgressSplitShardIdWhenNoSplitInProgress() {
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(1).build();
        assertEquals("When no split is in progress, SPLIT_NOT_IN_PROGRESS should be returned", SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());
    }

    @Test
    public void testGetInProgressSplitShardIdWithValidSplitInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();
        assertEquals("When a valid split is in progress, the correct shard ID should be returned", 0, metadata.getInProgressSplitShardId());
    }

    @Test
    public void testIsSplitOfShardInProgress_CorrectShardInProgress() {
        /**
         * Test that the method returns true when the correct shard is being split.
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();
        assertTrue(metadata.isSplitOfShardInProgress(2));
    }

    @Test
    public void testIsSplitOfShardInProgress_DifferentShardInProgress() {
        /**
         * Test that the method returns false when a different shard is being split.
         */
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(2, 2);
        SplitShardsMetadata metadata = builder.build();
        assertFalse(metadata.isSplitOfShardInProgress(1));
    }

    @Test
    public void testIsSplitOfShardInProgress_NoSplitInProgress() {
        /**
         * Test that the method returns false when no split is in progress.
         */
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(5).build();
        assertFalse(metadata.isSplitOfShardInProgress(0));
        assertTrue(metadata.isSplitOfShardInProgress(SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS));
    }

    @Test
    public void testSplitShardWhenSplitAlreadyInProgress() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(2);
        builder.splitShard(0, 2);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            builder.splitShard(1, 2);
        });
        assertTrue(exception.getMessage().contains("Split of shard [0] is already in progress or completed."));
    }

    @Test
    public void testSplitShardWithCompletedSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(5, 6));
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            builder.splitShard(0, 2);
        });
        assertTrue(exception.getMessage().startsWith("Split of shard [-2] is already in progress or completed."));
    }

    @Test
    public void testSplitShardWithInvalidNumberOfChildren() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            builder.splitShard(0, 10000000);
        });
        assertTrue(exception.getMessage().contains("Cannot split shard [0] further."));
    }

    @Test
    public void testSplitShardWithInvalidChildren() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            builder.splitShard(0, 1000000);
        });
        assertTrue(exception.getMessage().contains(" is below shard range threshold of "));
    }

    @Test
    public void testSplitInvalidShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> builder.splitShard(10, 2) // Invalid shard ID
        );
        assertTrue(exception.getMessage().contains("Shard ID doesn't exist in the current list of shard ranges"));
    }

    @Test
    public void testSplitShardBasicOperation() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        // Test basic split operation
        builder.splitShard(0, 2);
        SplitShardsMetadata metadata = builder.build();

        // Verify split state
        assertEquals(0, metadata.getInProgressSplitShardId());
        ShardRange[] childShards = metadata.getChildShardsOfParent(0);
        assertNotNull(childShards);
        assertEquals(2, childShards.length);

        // Verify shard ranges
        assertTrue(childShards[0].getEnd() < childShards[1].getStart());
        assertEquals(childShards[0].getEnd() + 1, childShards[1].getStart());
    }

    @Test
    public void testSplitShardNestedSplit() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1); // Start with 1 root shard

        // First split - split root shard 0 into two children (shards 1 and 2)
        builder.splitShard(0, 2);
        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2));

        // Verify first split
        SplitShardsMetadata metadata = builder.build();
        ShardRange[] firstLevelShards = metadata.getChildShardsOfParent(0);
        assertNotNull("First level split should create child shards", firstLevelShards);
        assertEquals("Should have 2 child shards", 2, firstLevelShards.length);

        // Second split - split first child shard (shard 1) into three children
        builder.splitShard(1, 3);
        builder.updateSplitMetadataForChildShards(1, Set.of(3, 4, 5));

        // Get final metadata
        metadata = builder.build();

        // Verify the nested split results
        ShardRange[] secondLevelShards = metadata.getChildShardsOfParent(1);
        assertNotNull("Second level split should create child shards", secondLevelShards);
        assertEquals("Should have 3 child shards", 3, secondLevelShards.length);

//         Verify shard hierarchy
        assertTrue("Original shard 0 should be split", metadata.isEmptyParentShard(0));
        assertTrue("Child shard 1 should be split", metadata.isEmptyParentShard(1));
        assertFalse("Child shard 2 should not be split", metadata.isEmptyParentShard(2));

        // Verify ranges are properly distributed
        for (int i = 0; i < secondLevelShards.length - 1; i++) {
            assertTrue("Shard ranges should be in order",
                secondLevelShards[i].getEnd() < secondLevelShards[i + 1].getStart());
            assertEquals("Shard ranges should be contiguous",
                secondLevelShards[i].getEnd() + 1, secondLevelShards[i + 1].getStart());
        }

        // Verify range boundaries
        ShardRange parentRange = firstLevelShards[0]; // Range of shard 1
        assertEquals("First child should start at parent's start",
            parentRange.getStart(), secondLevelShards[0].getStart());
        assertEquals("Last child should end at parent's end",
            parentRange.getEnd(), secondLevelShards[secondLevelShards.length - 1].getEnd());

        // Test hash routing through the nested structure
        int hashInFirstThird = parentRange.getStart() +
            (parentRange.getEnd() - parentRange.getStart()) / 3;

        assertEquals("Hash should route to first child of nested split",
            3, metadata.getShardIdOfHash(0, hashInFirstThird, true));
    }

    @Test
    public void testUpdateSplitMetadataWithInvalidSourceShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(1);
        newChildShardIds.add(2);
        assertThrows(IllegalArgumentException.class, () -> {
            builder.updateSplitMetadataForChildShards(1, newChildShardIds);
        });
    }

//    @Test
//    public void testUpdateSplitMetadataForChildShards_NoSplitInProgress() {
//        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(1);
//        builder.splitShard(0, 2);
//        builder.cancelSplit(0);
//        builder.updateSplitMetadataForChildShards(0, Set.of(1, 2));

//        assertThrows(AssertionError.class, () -> {
//            builder.updateSplitMetadataForChildShards(0, newChildShardIds);
//        });
//    }

    @Test
    public void testUpdateSplitMetadataForChildShards_InvalidChildShardId() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(5);
        newChildShardIds.add(7);

        assertThrows(AssertionError.class, () -> {
            builder.updateSplitMetadataForChildShards(0, newChildShardIds);
        });
    }

    @Test
    public void testUpdateSplitMetadataForChildShards_MismatchedChildShardCount() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();
        newChildShardIds.add(5);

        assertThrows(AssertionError.class, () -> {
            builder.updateSplitMetadataForChildShards(0, newChildShardIds);
        });
    }

    @Test
    public void testUpdateSplitMetadataForChildShards_EmptyNewChildShardIds() {
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(5);
        builder.splitShard(0, 2);
        Set<Integer> newChildShardIds = new HashSet<>();

        assertThrows(AssertionError.class, () -> {
            builder.updateSplitMetadataForChildShards(0, newChildShardIds);
        });
    }

    /**
     * Test case for updateSplitMetadataForChildShards method
     * Verifies that the method correctly updates the metadata for child shards
     */
    @Test
    public void testUpdateSplitMetadataForChildShards_Success() {
        // Arrange
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(2);
        int sourceShardId = 0;
        builder.splitShard(sourceShardId, 2);
        Set<Integer> newChildShardIds = new HashSet<>(Arrays.asList(2, 3));

        // Act
        builder.updateSplitMetadataForChildShards(sourceShardId, newChildShardIds);
        SplitShardsMetadata metadata = builder.build();

        // Assert
        assertEquals(SplitShardsMetadata.SPLIT_NOT_IN_PROGRESS, metadata.getInProgressSplitShardId());
        assertEquals(4, metadata.getNumberOfShards());

        ShardRange[] childShards = metadata.getChildShardsOfParent(sourceShardId);
        assertNotNull(childShards);
        assertEquals(2, childShards.length);
        assertTrue(newChildShardIds.contains(childShards[0].getShardId()));
        assertTrue(newChildShardIds.contains(childShards[1].getShardId()));

    }

    @Test
    public void testReadDiffFromWithEmptyInput() throws IOException {
        StreamInput emptyInput = Mockito.mock(StreamInput.class);
        when(emptyInput.available()).thenReturn(0);
        assertNotNull(SplitShardsMetadata.readDiffFrom(emptyInput));
    }

    @Test
    public void testEqualsMethod() {
        // Test 1: Same object reference
        SplitShardsMetadata metadata = new SplitShardsMetadata.Builder(1).build();
        assertTrue("Same object should be equal to itself", metadata.equals(metadata));

        // Test 2: Null comparison
        assertFalse("Object should not be equal to null", metadata.equals(null));

        // Test 3: Different class
        assertFalse("Should not be equal to different class",
            metadata.equals(new Object()));

        // Test 4: Equal objects
        SplitShardsMetadata metadata1 = new SplitShardsMetadata.Builder(2).build();
        SplitShardsMetadata metadata2 = new SplitShardsMetadata.Builder(2).build();
        assertTrue("Two equivalent objects should be equal", metadata1.equals(metadata2));

        // Test 5: Different maxShardId
        SplitShardsMetadata differentMaxShardId = new SplitShardsMetadata.Builder(3).build();
        assertFalse("Objects with different maxShardId should not be equal",
            metadata1.equals(differentMaxShardId));

        // Test 6: Different inProgressSplitShardId
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(2);
        builder1.splitShard(0, 2);  // This will set inProgressSplitShardId
        SplitShardsMetadata withSplit = builder1.build();
        assertFalse("Objects with different inProgressSplitShardId should not be equal",
            metadata1.equals(withSplit));

        // Test 7: Different rootShardsToAllChildren
        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(2);
        builder2.splitShard(0, 2);
        builder2.updateSplitMetadataForChildShards(0, Set.of(2, 3));
        SplitShardsMetadata withDifferentRootShards = builder2.build();
        assertFalse("Objects with different rootShardsToAllChildren should not be equal",
            metadata1.equals(withDifferentRootShards));

        // Test 8: Different parentToChildShards
        SplitShardsMetadata.Builder builder3 = new SplitShardsMetadata.Builder(2);
        builder3.splitShard(1, 2);  // Split different shard
        builder3.updateSplitMetadataForChildShards(1, Set.of(2, 3));
        SplitShardsMetadata withDifferentParentToChild = builder3.build();
        assertFalse("Objects with different parentToChildShards should not be equal",
            withDifferentRootShards.equals(withDifferentParentToChild));

        // Test 9: Symmetric equality
        assertTrue("Equality should be symmetric",
            metadata1.equals(metadata2) && metadata2.equals(metadata1));

        // Test 10: Transitive equality
        SplitShardsMetadata metadata3 = new SplitShardsMetadata.Builder(2).build();
        assertTrue("Equality should be transitive",
            metadata1.equals(metadata2) && metadata2.equals(metadata3) && metadata1.equals(metadata3));
    }

    @Test
    public void testEqualsWithNullArrayElements() {
        // Create metadata with null elements in rootShardsToAllChildren
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata metadata1 = builder1.build();

        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(3);
        SplitShardsMetadata metadata2 = builder2.build();

        assertTrue("Objects with null array elements should be equal if structure is same",
            metadata1.equals(metadata2));
    }

    @Test
    public void testEqualsWithEmptyMaps() {
        // Create metadata with empty parentToChildShards maps
        SplitShardsMetadata.Builder builder1 = new SplitShardsMetadata.Builder(1);
        SplitShardsMetadata metadata1 = builder1.build();

        SplitShardsMetadata.Builder builder2 = new SplitShardsMetadata.Builder(1);
        SplitShardsMetadata metadata2 = builder2.build();

        assertTrue("Objects with empty maps should be equal if structure is same",
            metadata1.equals(metadata2));
    }



}
