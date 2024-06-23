/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.CheckedRunnable;
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
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.indices.recovery.RecoveryTargetHandler;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.RemoteStoreReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.SegmentReplicationTarget;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
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
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY;

public class InPlaceShardSplitRecoveryTargetHandler implements RecoveryTargetHandler, Closeable {
    private final Map<ShardId, RecoveryTarget> recoveryTargets;
    private final List<InPlaceShardRecoveryContext> recoveryContexts;
    private final Set<String> childShardsAllocationIds;
    private final IndexShard sourceShard;
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
                                                  final IndexShard sourceShard) {
        Map<ShardId, RecoveryTarget> recoveryTargetMap = new HashMap<>();
        indexShards.forEach(shard -> {
            recoveryTargetMap.put(shard.shardId(),
                new RecoveryTarget(shard, sourceNode, unSupportedTargetListener, cancellableThreads, true));
        });
        this.sourceShard = sourceShard;
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
                    childShard.cleanUpRemoteDirectories();
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

//    public void blockOpsAndForceSegmentFileSync() {
//        recoveryContexts.forEach(context -> {
//            cancellableThreads.checkForCancel();
//            CheckedRunnable<IOException> forceSegmentSync = () -> internalForceSegmentSync(context.getIndexShard());
//            try {
//                context.getIndexShard().blockOperationsAndExecute(forceSegmentSync);
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
//        });
//    }

//    public void internalForceSegmentSync(IndexShard childShard) {
//        SegmentReplicationTarget segmentReplicationTarget = new SegmentReplicationTarget(
//            childShard,
//            sourceShard,
//            childShard.getLatestReplicationCheckpoint(),
//            segRepFactory.get(sourceShard),
//            null
//        );
//
//        CountDownLatch latch = new CountDownLatch(1);
//        AtomicReference<Exception> replicationFailure = new AtomicReference<>();
//        LatchedActionListener<Void> latchedActionListener = new LatchedActionListener<>(
//            ActionListener.wrap(res -> childShard.resetToWriteableEngine(),
//                replicationFailure::set), latch
//        );
//
//        segmentReplicationTarget.startReplication(latchedActionListener);
//        try {
//            latch.await();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (replicationFailure.get() != null) {
//            throw new RuntimeException(replicationFailure.get());
//        }
//    }

//    private static class RemoteReplicationSource extends RemoteStoreReplicationSource {
//        public RemoteReplicationSource(IndexShard sourceShard) {
//            super(sourceShard);
//        }
//
//        @Override
//        protected void syncFromRemote(
//            List<StoreFileMetadata> filesToFetch,
//            IndexShard targetShard,
//            BiConsumer<String, Long> fileProgressTracker,
//            ActionListener<GetSegmentFilesResponse> listener,
//            List<String> toSyncSegmentFiles
//        ) throws IOException {
//
//        }
//    }

    @Override
    public void forceSegmentFileSync() {
//        if (sourceShard.indexSettings().isSegRepEnabled() == false) {
//            return;
//        }
//
//        recoveryContexts.forEach(context -> {
//            cancellableThreads.checkForCancel();
//            internalForceSegmentSync(context.getIndexShard());
//        });
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

    public void receiveFilesAndSplit(Store store, StoreFileMetadata[] files, long maxSeqNo,
                                     SplitCommitMetadata splitCommitMetadata,
                                     long maxUnsafeAutoIdTimestamp)
        throws Exception {

        Releasable releaseSourceRemote = null;
        Store remoteStore = sourceShard.remoteStore();
        if (remoteStore != null) {
            releaseSourceRemote = Objects.requireNonNull(storeAcquirer.get()).apply(sourceShard.remoteStore());
        }

        try {
            for (InPlaceShardRecoveryContext context : recoveryContexts) {
                try (Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(context.getIndexShard().store())) {
                    Directory directory = syncLocalDirectory(store, files, context.getIndexShard());
                    cancellableThreads.checkForCancel();
                    split(maxSeqNo, maxUnsafeAutoIdTimestamp, directory, context);

                    if (remoteStore != null) {
                        cancellableThreads.checkForCancel();
                        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
                        ReplicationCheckpoint replicationCheckpoint = context.getIndexShard().computeReplicationCheckpoint(
                            segmentInfos, sourceShard);
                        Collection<String> localSegments = segmentInfos.files(true);
                        long translogFileGen = splitCommitMetadata.getTranslogGen();

                        // Perform a remote store to remote store copy if possible or else fallback to upload from local.
                        try (Releasable ignoreRemote = Objects.requireNonNull(storeAcquirer.get()).apply(remoteStore)) {
                            context.getIndexShard().copySegmentsAndMetadataToRemote(
                                sourceShard.getRemoteDirectory(),
                                directory,
                                segmentInfos,
                                replicationCheckpoint,
                                localSegments,
                                translogFileGen,
                                splitCommitMetadata.getMetadataTuple().v2()
                            );
                        }
                    }
                }
            }
        } finally {
            if (releaseSourceRemote != null) {
                releaseSourceRemote.close();
            }
        }

    }

    public Directory syncLocalDirectory(Store store, StoreFileMetadata[] files, IndexShard childShard)
        throws IOException {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length));

        Store childShardStore = childShard.store();
        HardlinkCopyDirectoryWrapper hardLinkOrCopyTarget = new HardlinkCopyDirectoryWrapper(
            childShardStore.directory());
        for (StoreFileMetadata file : files) {
            long sourceFileChecksum;
            try (IndexInput indexInput = store.directory().openInput(file.name(), IOContext.DEFAULT)) {
                sourceFileChecksum = CodecUtil.retrieveChecksum(indexInput);
            }
            if (childShard.localDirectoryContains(childShardStore.directory(), file.name(),
                sourceFileChecksum) == false) {
                hardLinkOrCopyTarget.copyFrom(store.directory(), file.name(), file.name(), IOContext.DEFAULT);
            }
        }

        return hardLinkOrCopyTarget;
    }

    private void split(long maxSeqNo, long maxUnsafeAutoIdTimestamp, Directory childShardDirectory,
                       InPlaceShardRecoveryContext context) throws IOException {
        Tuple<Boolean, Directory> addIndexSplitDirectory = new Tuple<>(false, childShardDirectory);
        StoreRecovery.addIndices(
            context.getRecoveryState().getIndex(),
            sourceShard.getIndexSort(),
            new Directory[]{childShardDirectory},
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

        String sourceTranslogUUID;
        try {
            sourceTranslogUUID = sourceShard.store().getMetadata().getCommitUserData().get(TRANSLOG_UUID_KEY);
        } catch (Exception ex) {
            listener.onFailure(ex);
            return;
        }

        try {
            for (InPlaceShardRecoveryContext context : recoveryContexts) {
                try (Releasable ignore = Objects.requireNonNull(storeAcquirer.get()).apply(context.getIndexShard().store())) {
                    context.getIndexShard().store().bootstrapNewHistory();

                    try {
                        // Associate store with source translog UUID. This may not be required for DocRep replication mode.
                        context.getIndexShard().store().associateIndexWithNewTranslog(sourceTranslogUUID);
                    } catch (Exception ex) {
                        listener.onFailure(ex);
                        return;
                    }
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
