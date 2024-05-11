/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.StoreRecovery;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.recovery.RecoveryTargetHandler;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class InPlaceShardSplitRecoveryTargetHandler implements RecoveryTargetHandler, Closeable {
    private final Map<ShardId, RecoveryTarget> recoveryTargets;
    private final List<InPlaceShardRecoveryContext> recoveryContexts;
    private final Set<String> childShardsAllocationIds;
    private final IndexShard sourceShard;
    private final SegmentReplicationSourceFactory segRepFactory;
    private final CancellableThreads cancellableThreads;
    private final SetOnce<Function<Store, Releasable>> storeAcquirer = new SetOnce<>();

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
                new RecoveryTarget(shard, sourceNode, unSupportedTargetListener, cancellableThreads, true));
        });
        this.sourceShard = sourceShard;
        this.segRepFactory = segRepFactory;
        this.recoveryTargets = Collections.unmodifiableMap(recoveryTargetMap);
        this.recoveryContexts = recoveryContexts;
        this.childShardsAllocationIds = childShardsAllocationIds;
        this.cancellableThreads = cancellableThreads;
    }

    public void initStoreAcquirer(Function<Store, Releasable> storeAcquirer) {
        this.storeAcquirer.set(storeAcquirer);
    }

    public void cleanShardDirectoriesForTargets() throws IOException {
        for (InPlaceShardRecoveryContext context : recoveryContexts) {
            cancellableThreads.checkForCancel();
            IndexShard childShard = context.getIndexShard();
            try(Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(childShard.store())) {
                cleanUpStoreDirectory(childShard.store());
            }
            Store remoteStore = childShard.remoteStore();
            if (remoteStore != null) {
                try(Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(remoteStore)) {
                    cleanUpStoreDirectory(remoteStore);
                }
            }
        }
    }

    public void cleanUpStoreDirectory(Store store) throws IOException {
        Directory storeDirectory = store.directory();
        for (String file : storeDirectory.listAll()) {
            storeDirectory.deleteFile(file);
        }
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

    public void refresh() {
        recoveryContexts.forEach(context -> context.getIndexShard().refresh("In-Place split"));
    }

    public void indexTranslogOperationsOnShards(List<Translog.Operation> operations, int totalTranslogOps,
                                        long maxSeenAutoIdTimestampOnPrimary, long maxSeqNoOfUpdatesOrDeletesOnPrimary,
                                        RetentionLeases retentionLeases, long mappingVersionOnPrimary,
                                                ActionListener<Collection<BatchOperationsResult>> listener) {

        GroupedActionListener<BatchOperationsResult> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(listener::onResponse, listener::onFailure),
            recoveryContexts.size()
        );

        recoveryContexts.forEach(context -> {
            RecoveryTarget recoveryTarget = recoveryTargets.get(context.getIndexShard().shardId());
            cancellableThreads.checkForCancel();
            String targetAllocationId = context.getIndexShard().routingEntry().allocationId().getId();
            ActionListener<Long> checkpointListener = ActionListener.wrap(checkpoint -> {
                groupedActionListener.onResponse(new BatchOperationsResult(checkpoint, targetAllocationId));
            }, groupedActionListener::onFailure);
            recoveryTarget.indexTranslogOperations(
                operations,
                totalTranslogOps,
                maxSeenAutoIdTimestampOnPrimary,
                maxSeqNoOfUpdatesOrDeletesOnPrimary,
                retentionLeases,
                mappingVersionOnPrimary,
                checkpointListener
            );
        });
    }

    @Override
    public void close() throws IOException {
        recoveryTargets.values().forEach(recoveryTarget ->
            IOUtils.closeWhileHandlingException(recoveryTarget::decRef));
    }

    public static class BatchOperationsResult {
        final long checkpoint;
        final String allocationId;

        public BatchOperationsResult(long checkpoint, String allocationId) {
            this.checkpoint = checkpoint;
            this.allocationId = allocationId;
        }
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
    public void indexTranslogOperations(
        List<Translog.Operation> operations, int totalTranslogOps, long maxSeenAutoIdTimestampOnPrimary,
        long maxSeqNoOfUpdatesOrDeletesOnPrimary, RetentionLeases retentionLeases, long mappingVersionOnPrimary,
        ActionListener<Long> listener) {
        throw new UnsupportedOperationException("Single shard method for indexing translog operations " +
            "batch is not supported in in-place recovery");
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes,
                                int totalTranslogOps, ActionListener<Void> listener) {
        GroupedActionListener<Void> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(res -> listener.onResponse(null), listener::onFailure),
            recoveryContexts.size()
        );

        recoveryTargets.values().forEach(recoveryTarget -> recoveryTarget.receiveFileInfo(
            phase1FileNames,
            phase1FileSizes,
            phase1ExistingFileNames,
            phase1ExistingFileSizes,
            totalTranslogOps,
            groupedActionListener)
        );
    }

    public void receiveFilesAndSplit(Store store, StoreFileMetadata[] files, long maxSeqNo, long maxUnsafeAutoIdTimestamp) throws IOException {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length));
        for (InPlaceShardRecoveryContext context : recoveryContexts) {
            cancellableThreads.checkForCancel();
            try (Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(context.getIndexShard().store())) {
                Store childShardStore = context.getIndexShard().store();
                HardlinkCopyDirectoryWrapper hardLinkOrCopyTarget = new HardlinkCopyDirectoryWrapper(
                    childShardStore.directory());
                for (StoreFileMetadata file : files) {
                    hardLinkOrCopyTarget.copyFrom(store.directory(), file.name(), file.name(), IOContext.DEFAULT);
                }

                Tuple<Boolean, Directory> addIndexSplitDirectory = new Tuple<>(false, hardLinkOrCopyTarget);
                StoreRecovery.addIndices(
                    context.getRecoveryState().getIndex(),
                    sourceShard.getIndexSort(),
                    new Directory[]{childShardStore.directory()},
                    maxSeqNo,
                    maxUnsafeAutoIdTimestamp,
                    sourceShard.indexSettings().getIndexMetadata(),
                    context.getIndexShard().shardId().id(),
                    true,
                    context.getIndexShard().mapperService().hasNested(),
                    addIndexSplitDirectory,
                    (shardId) -> true,
                    IndexWriterConfig.OpenMode.APPEND
                );
            }
        }
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content, boolean lastChunk,
                               int totalTranslogOps, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("In-place shard split recovery doesn't involve any file copy");
    }

    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata, ActionListener<Void> listener) {
        GroupedActionListener<Void> groupedActionListener = new GroupedActionListener<>(
            ActionListener.wrap(res -> listener.onResponse(null), listener::onFailure),
            recoveryContexts.size()
        );

        try {
            for (InPlaceShardRecoveryContext context : recoveryContexts) {
                try (Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(context.getIndexShard().store())) {
                    context.getIndexShard().store().bootstrapNewHistory();
                    ShardId shardId = context.getIndexShard().shardId();
                    RecoveryTarget recoveryTarget = recoveryTargets.get(shardId);
                    recoveryTarget.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, groupedActionListener);
                }
            }
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public void onDone() {
        recoveryContexts.forEach(context -> context.getIndexShard().postRecovery("In-Place shard split completed"));
    }

}
