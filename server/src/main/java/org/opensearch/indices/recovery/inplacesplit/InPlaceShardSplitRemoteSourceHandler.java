///*
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
//package org.opensearch.indices.recovery.inplacesplit;
//
//import org.apache.lucene.index.IndexCommit;
//import org.apache.lucene.store.Directory;
//import org.opensearch.action.StepListener;
//import org.opensearch.common.SetOnce;
//import org.opensearch.common.collect.Tuple;
//import org.opensearch.common.concurrent.GatedCloseable;
//import org.opensearch.common.lease.Releasable;
//import org.opensearch.common.unit.TimeValue;
//import org.opensearch.common.util.CancellableThreads;
//import org.opensearch.common.util.io.IOUtils;
//import org.opensearch.core.action.ActionListener;
//import org.opensearch.core.index.shard.ShardId;
//import org.opensearch.index.engine.Engine;
//import org.opensearch.index.seqno.RetentionLeases;
//import org.opensearch.index.seqno.SequenceNumbers;
//import org.opensearch.index.shard.IndexShard;
//import org.opensearch.index.store.Store;
//import org.opensearch.index.store.StoreFileMetadata;
//import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
//import org.opensearch.index.translog.Translog;
//import org.opensearch.indices.RunUnderPrimaryPermit;
//import org.opensearch.indices.recovery.RecoveryResponse;
//import org.opensearch.indices.recovery.RecoverySourceHandler;
//import org.opensearch.indices.recovery.RemoteStorePeerRecoverySourceHandler;
//import org.opensearch.indices.recovery.StartRecoveryRequest;
//import org.opensearch.repositories.RepositoriesService;
//import org.opensearch.threadpool.ThreadPool;
//import org.opensearch.transport.Transports;
//
//import java.io.Closeable;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.function.Consumer;
//import java.util.function.IntSupplier;
//
//public class InPlaceShardSplitRemoteSourceHandler extends RecoverySourceHandler {
//
//    private final List<InPlaceShardRecoveryContext> recoveryContexts;
//    private final InPlaceShardSplitRecoveryTargetHandler recoveryTarget;
//    private final IndexShard sourceShard;
//    private final RemoteStorePeerRecoverySourceHandler delegatingRecoveryHandler;
//    private final List<ShardId> shardIds;
//    private final Set<String> childShardsAllocationIds;
//    private final RepositoriesService repositoriesService;
//    private final SetOnce<Tuple<String, RemoteSegmentMetadata>> metadataTuple = new SetOnce<>();
//    private final String IN_PLACE_SHARD_SPLIT_RECOVERY_NAME = "in-place-shard-split-recovery";
//
//    public InPlaceShardSplitRemoteSourceHandler(
//        IndexShard sourceShard,
//        ThreadPool threadPool,
//        InPlaceShardSplitRecoveryTargetHandler recoveryTarget,
//        RemoteStorePeerRecoverySourceHandler delegatingRecoveryHandler,
//        StartRecoveryRequest request,
//        int fileChunkSizeInBytes,
//        int maxConcurrentFileChunks,
//        int maxConcurrentOperations,
//        CancellableThreads cancellableThreads,
//        List<InPlaceShardRecoveryContext> recoveryContexts,
//        List<ShardId> shardIds,
//        Set<String> childShardsAllocationIds,
//        RepositoriesService repositoriesService
//    ) {
//        super(sourceShard, recoveryTarget,
//            threadPool, request, fileChunkSizeInBytes, maxConcurrentFileChunks,
//            maxConcurrentOperations, true, cancellableThreads);
//
//        this.resources.add(recoveryTarget);
//        this.recoveryContexts = recoveryContexts;
//        this.sourceShard = sourceShard;
//        this.delegatingRecoveryHandler = delegatingRecoveryHandler;
//        this.shardIds = shardIds;
//        this.childShardsAllocationIds = childShardsAllocationIds;
//        this.recoveryTarget = recoveryTarget;
//        this.repositoriesService = repositoriesService;
//
//        recoveryTarget.initStoreAcquirer((requestStore) -> {
//            Releasable releasable = acquireStore(requestStore);
//            resources.add(releasable);
//            return releasable;
//        });
//    }
//
//    @Override
//    protected void innerRecoveryToTarget(ActionListener<RecoveryResponse> listener,
//                                         Consumer<Exception> onFailure) throws IOException {
//        // Clean up shard directories if previous shard closures failed.
//        cleanupChildShardDirectories();
//
//        List<Releasable> delayedStaleCommitDeleteOps = sourceShard.delayStaleCommitDeletions();
//        resources.addAll(delayedStaleCommitDeleteOps);
//        Closeable translogRetentionLock = sourceShard.acquireTranslogRetentionLock();
//        resources.add(translogRetentionLock);
//
//        GatedCloseable<IndexCommit> lastCommit = acquireLastCommit(sourceShard,false);
//        resources.add(lastCommit);
//        Releasable releaseStore = acquireStore(sourceShard.remoteStore());
//        resources.add(releaseStore);
//
//        Tuple<String, RemoteSegmentMetadata> fetchedMetadataTuple = sourceShard.getMetadataContentForCommit(
//            sourceShard.getOperationPrimaryTerm(),
//            lastCommit.get().getGeneration());
//        ensureMetadataHasAllSegmentsFromCommit(lastCommit.get(), fetchedMetadataTuple.v2());
//        this.metadataTuple.set(fetchedMetadataTuple);
//
//        final StepListener<SendFileResult> sendFileStep = new StepListener<>();
//        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
//        final StepListener<List<SendSnapshotResult>> sendSnapshotStep = new StepListener<>();
//
//        postSendFileComplete(sendFileStep, lastCommit, releaseStore, delayedStaleCommitDeleteOps);
//        long startingSeqNo = Long.parseLong(lastCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
//
//        assert Transports.assertNotTransportThread(this + "[phase1]");
//        phase1(lastCommit.get(), startingSeqNo, () -> 0, sendFileStep, true);
//
//        prepareEngine(sendFileStep, prepareEngineStep, (int) (sourceShard.getLastKnownGlobalCheckpoint() - startingSeqNo), onFailure);
//        initiateTracking();
//
//        prepareEngineStep.whenComplete(prepareEngineTime -> {
//            // Do a refresh here. In case of remote replication, refresh will upload segments to remote child directories.
//            recoveryTarget.refresh();
//
//            logger.debug("prepareEngineStep completed");
//            assert Transports.assertNotTransportThread(this + "[phase2]");
//            initiateTracking();
//
//            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
//            final Translog.Snapshot phase2Snapshot = phase2Snapshot(startingSeqNo, IN_PLACE_SHARD_SPLIT_RECOVERY_NAME);
//            resources.add(phase2Snapshot);
//            if (logger.isTraceEnabled()) {
//                logger.trace("snapshot translog for recovery; current size is [{}]", countNumberOfHistoryOperations(startingSeqNo));
//            }
//            final long mappingVersionOnPrimary = shard.indexSettings().getIndexMetadata().getMappingVersion();
//            phase2(
//                startingSeqNo,
//                endingSeqNo,
//                phase2Snapshot,
//                shard.getMaxSeenAutoIdTimestamp(),
//                shard.getMaxSeqNoOfUpdatesOrDeletes(),
//                shard.getRetentionLeases(),
//                mappingVersionOnPrimary,
//                sendSnapshotStep
//            );
//        }, onFailure);
//
//        StepListener<Void> finalizeStep = new StepListener<>();
//        finalizeStep.whenComplete(r -> cleanUpMaybeRemoteOnFinalize(), onFailure);
//        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStepWithEmptyResult(), prepareEngineStep, finalizeStep, onFailure);
//    }
//
//    public void cleanupChildShardDirectories() throws IOException {
//        recoveryTarget.cleanShardDirectoriesForTargets();
//    }
//
//    private void ensureMetadataHasAllSegmentsFromCommit(IndexCommit indexCommit, RemoteSegmentMetadata metadata) throws IOException {
//        Collection<String> files = indexCommit.getFileNames();
//        List<String> missingFiles = new ArrayList<>();
//        for (String file : files) {
//            if (metadata.getMetadata().containsKey(file) == false) {
//                missingFiles.add(file);
//            }
//        }
//
//        if (missingFiles.isEmpty() == false) {
//            throw new IllegalStateException("Missing segments in remote segments metadata. Missing files ["
//                + missingFiles + "] for commit generation [" + indexCommit.getGeneration() + "]");
//        }
//    }
//
//    @Override
//    public int countNumberOfHistoryOperations(long startingSeqNo) throws IOException {
//        return 0;
//    }
//
//    @Override
//    public Closeable acquireRetentionLock() {
//        return null;
//    }
//
//    @Override
//    public Translog.Snapshot phase2Snapshot(long startingSeqNo, String recoveryName){
//        return null;
//    }
//
//    @Override
//    public boolean shouldSkipCreateRetentionLeaseStep() {
//        return true;
//    }
//
//    protected void sendFiles(Store store, StoreFileMetadata[] files, IntSupplier translogOps,
//                             ActionListener<Void> listener, IndexCommit snapshot) {
//        try {
//            long maxSeqNo = Long.parseLong(snapshot.getUserData().get(SequenceNumbers.MAX_SEQ_NO));
//            long maxUnsafeAutoIdTimestamp = Long.parseLong(snapshot.getUserData().get(
//                Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID));
//            recoveryTarget.receiveFilesAndSyncLocalAndRemote(store, files, maxSeqNo, maxUnsafeAutoIdTimestamp,
//                metadataTuple.get());
//            listener.onResponse(null);
//        } catch (Exception e) {
//            listener.onFailure(e);
//        }
//    }
//
//    protected void postSendFileComplete(
//        StepListener<SendFileResult> sendFileStep,
//        GatedCloseable<IndexCommit> wrappedSafeCommit,
//        Releasable releaseStore,
//        List<Releasable> delayedStaleCommitOps
//    ) {
//        sendFileStep.whenComplete(r -> {
//            logger.debug("sendFileStep completed");
//            delayedStaleCommitOps.forEach(Releasable::close);
//            IOUtils.close(wrappedSafeCommit, releaseStore);
//        }, e -> {
//            try {
//                IOUtils.close(wrappedSafeCommit, releaseStore);
//                delayedStaleCommitOps.forEach(Releasable::close);
//            } catch (final IOException ex) {
//                logger.warn("releasing snapshot caused exception", ex);
//            }
//        });
//    }
//
//    public void prepareEngine(StepListener<SendFileResult> sendFileStep,
//                                               StepListener<TimeValue> prepareEngineStep,
//                                               int totalTranslogOps,
//                                               Consumer<Exception> onFailure) {
//        sendFileStep.whenComplete(r -> {
//            logger.debug("sendFileStep completed");
//            assert Transports.assertNotTransportThread(this + "[prepareTargetForTranslog]");
//            // For a sequence based recovery, the target can keep its local translog
//            prepareTargetForTranslog(totalTranslogOps, prepareEngineStep);
//        }, onFailure);
//    }
//
//    private void initiateTracking() {
//        cancellableThreads.checkForCancel();
//        List<String> allocationIDs = new ArrayList<>();
//        recoveryContexts.forEach(context -> allocationIDs.add(context.getIndexShard()
//            .routingEntry().allocationId().getId()));
//
//        RunUnderPrimaryPermit.run(
//            () -> shard.initiateTrackingOfChildShards(allocationIDs),
//            shardId + " initiating tracking of " + allocationIDs,
//            shard,
//            cancellableThreads,
//            logger
//        );
//    }
//
//    @Override
//    protected OperationBatchSender createSender(
//        final long startingSeqNo,
//        final long endingSeqNo,
//        final Translog.Snapshot snapshot,
//        final long maxSeenAutoIdTimestamp,
//        final long maxSeqNoOfUpdatesOrDeletes,
//        final RetentionLeases retentionLeases,
//        final long mappingVersion,
//        StepListener<Void> sendListener
//    ) {
//        return new AllShardsOperationBatchSender(startingSeqNo, endingSeqNo, snapshot, maxSeenAutoIdTimestamp,
//            maxSeqNoOfUpdatesOrDeletes, retentionLeases, mappingVersion, sendListener);
//    }
//
//    protected class AllShardsOperationBatchSender extends OperationBatchSender {
//        private final Map<String, AtomicLong> targetLocalCheckpoints = new HashMap<>();
//
//        protected AllShardsOperationBatchSender(
//            long startingSeqNo, long endingSeqNo, Translog.Snapshot snapshot,
//            long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes,
//            RetentionLeases retentionLeases, long mappingVersion, ActionListener<Void> listener) {
//            super(startingSeqNo, endingSeqNo, snapshot, maxSeenAutoIdTimestamp,
//                maxSeqNoOfUpdatesOrDeletes, retentionLeases, mappingVersion, listener);
//
//            childShardsAllocationIds.forEach(childShardsAllocationId -> {
//                targetLocalCheckpoints.put(childShardsAllocationId, new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED));
//            });
//        }
//
//        @Override
//        protected void executeChunkRequest(OperationChunkRequest request, ActionListener<Void> listener) {
//            cancellableThreads.checkForCancel();
//            recoveryTarget.indexTranslogOperationsOnShards(
//                request.getOperations(),
//                snapshot.totalOperations(),
//                maxSeenAutoIdTimestamp,
//                maxSeqNoOfUpdatesOrDeletes,
//                retentionLeases,
//                mappingVersion,
//                ActionListener.delegateFailure(listener, (l, allocationCheckpoints) -> {
//                    allocationCheckpoints.forEach(allocationCheckpoint -> {
//                        targetLocalCheckpoints.get(allocationCheckpoint.allocationId).updateAndGet(curr ->
//                            SequenceNumbers.max(curr, allocationCheckpoint.checkpoint));
//                    });
//                    l.onResponse(null);
//                })
//            );
//        }
//    }
//
//    private void cleanUpMaybeRemoteOnFinalize() {
//        Store remoteStore = sourceShard.remoteStore();
//        if (remoteStore != null) {
//            try(Releasable releasable = acquireStore(sourceShard.remoteStore())) {
//                resources.add(releasable);
//                Directory storeDirectory = remoteStore.directory();
//                for (String file : storeDirectory.listAll()) {
//                    storeDirectory.deleteFile(file);
//                }
//            } catch (IOException e) {
//                logger.error("Failed to cleanup source shard remote directory", e);
//            }
//        }
//    }
//}
