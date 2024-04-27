/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.recovery.RecoveryTargetHandler;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class InPlaceShardSplitRecoveryTargetHandler implements RecoveryTargetHandler {
    private static final Logger logger = LogManager.getLogger(PeerRecoveryTargetService.class);
    private final Map<ShardId, RecoveryTarget> recoveryTargets;
    private final List<InPlaceShardRecoveryContext> recoveryContexts;
    private final Set<String> childShardsAllocationIds;
    private final IndexShard sourceShard;
    private final SegmentReplicationSourceFactory segRepFactory;
    private final CancellableThreads cancellableThreads;

    private final ReplicationListener unSupportedTargetListener = new ReplicationListener() {
        @Override
        public void onDone(ReplicationState state) {
            throw new UnsupportedOperationException(
                "Recovery done callback is not supported on a target in in-place shard split recovery");
        }

        @Override
        public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
            throw new UnsupportedOperationException(
                "Recovery failure callback is not supported on a target in in-place shard split recovery");
        }
    };

    public InPlaceShardSplitRecoveryTargetHandler(final List<IndexShard> indexShards,
                                                  final DiscoveryNode sourceNode,
                                                  final CancellableThreads cancellableThreads,
                                                  final List<InPlaceShardRecoveryContext> recoveryContexts,
                                                  final Set<String> childShardsAllocationIds,
                                                  final IndexShard sourceShard,
                                                  final SegmentReplicationSourceFactory segRepFactory) {
        Map<ShardId, RecoveryTarget> recoveryTargetMap = new HashMap<>();
        indexShards.forEach(shard -> {
            recoveryTargetMap.put(shard.shardId(),
                new RecoveryTarget(shard, sourceNode, unSupportedTargetListener, cancellableThreads, false));
        });
        this.sourceShard = sourceShard;
        this.segRepFactory = segRepFactory;
        this.recoveryTargets = Collections.unmodifiableMap(recoveryTargetMap);
        this.recoveryContexts = recoveryContexts;
        this.childShardsAllocationIds = childShardsAllocationIds;
        this.cancellableThreads = cancellableThreads;
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        GroupedActionListener<Void> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(res -> listener.onResponse(null), listener::onFailure),
            recoveryTargets.size()
        );
        recoveryTargets.values().forEach(recoveryTarget -> {
            cancellableThreads.checkForCancel();
            recoveryTarget.prepareForTranslogOperations(totalTranslogOps, groupedActionListener);
        });
    }

    @Override
    public void forceSegmentFileSync() {
        if (sourceShard.indexSettings().isSegRepEnabled() == false) {
            return;
        }

        recoveryContexts.forEach(context -> {
            cancellableThreads.checkForCancel();
            SegmentReplicationTarget segmentReplicationTarget = new SegmentReplicationTarget(
                context.getIndexShard(),
                sourceShard,
                context.getIndexShard().getLatestReplicationCheckpoint(),
                segRepFactory.get(sourceShard),
                null
            );

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> replicationFailure = new AtomicReference<>();
            LatchedActionListener<Void> latchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(res -> context.getIndexShard().resetToWriteableEngine(),
                    replicationFailure::set), latch
            );

            segmentReplicationTarget.startReplication(latchedActionListener);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (replicationFailure.get() != null) {
                throw new RuntimeException(replicationFailure.get());
            }
        });
    }

    @Override
    public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
        GroupedActionListener<Void> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(res -> listener.onResponse(null), listener::onFailure),
            recoveryContexts.size()
        );
        recoveryTargets.values().forEach(recoveryTarget -> {
            cancellableThreads.checkForCancel();
            recoveryTarget.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, groupedActionListener);
        });
    }

    @Override
    public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext) {
        recoveryTargets.values().forEach(recoveryTarget -> recoveryTarget.handoffPrimaryContext(primaryContext));
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps,
                                        long maxSeenAutoIdTimestampOnPrimary, long maxSeqNoOfUpdatesOrDeletesOnPrimary,
                                        RetentionLeases retentionLeases, long mappingVersionOnPrimary, ActionListener<Long> listener) {
        if (totalTranslogOps == 0) {
            return;
        }

        GroupedActionListener<Long> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(res -> listener.onResponse(null), listener::onFailure),
            recoveryContexts.size()
        );

        recoveryTargets.values().forEach(recoveryTarget -> {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperations(
                operations,
                totalTranslogOps,
                maxSeenAutoIdTimestampOnPrimary,
                maxSeqNoOfUpdatesOrDeletesOnPrimary,
                retentionLeases,
                mappingVersionOnPrimary,
                groupedActionListener
            );
        });
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content, boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("In-place shard split recovery doesn't involve any file copy");
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("In-place shard split recovery doesn't involve any receiveFileInfo operation");
    }

    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("In-place shard split recovery doesn't involve any cleanFiles operation");
    }
}
