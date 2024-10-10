/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch] Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.recovery.RecoveryResponse;
import org.opensearch.indices.recovery.RecoverySourceHandler;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

public class InPlaceShardSplitRecoverySourceHandler extends RecoverySourceHandler {
    private final List<InPlaceShardRecoveryContext> recoveryContexts;
    private final InPlaceShardSplitRecoveryTargetHandler recoveryTarget;
    private final IndexShard sourceShard;
    private final RecoverySourceHandler delegatingRecoveryHandler;
    private final List<ShardId> shardIds;
    private final Set<String> childShardsAllocationIds;
    private final SetOnce<SplitCommitMetadata> splitCommitMetadata = new SetOnce<>();
    private final Logger logger;

    public InPlaceShardSplitRecoverySourceHandler(
        IndexShard sourceShard,
        InPlaceShardSplitRecoveryTargetHandler recoveryTarget,
        RecoverySourceHandler delegatingRecoveryHandler,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations,
        CancellableThreads cancellableThreads,
        List<InPlaceShardRecoveryContext> recoveryContexts,
        List<ShardId> shardIds,
        Set<String> childShardsAllocationIds
    ) {
        super(sourceShard, recoveryTarget,
            sourceShard.getThreadPool(), request, fileChunkSizeInBytes, maxConcurrentFileChunks,
            maxConcurrentOperations, true, cancellableThreads);
        this.logger = Loggers.getLogger(InPlaceShardSplitRecoverySourceHandler.class, request.shardId(),
            "splitting to " + shardIds);
        this.resources.add(recoveryTarget);
        this.recoveryContexts = recoveryContexts;
        this.sourceShard = sourceShard;
        this.delegatingRecoveryHandler = delegatingRecoveryHandler;
        this.shardIds = shardIds;
        this.childShardsAllocationIds = childShardsAllocationIds;
        this.recoveryTarget = recoveryTarget;

        recoveryTarget.initStoreAcquirer((requestStore) -> {
            Releasable releasable = acquireStore(requestStore);
            resources.add(releasable);
            return releasable;
        });
    }

    public IndexShard getSourceShard() {
        return sourceShard;
    }

    public List<Closeable> getAdditionalResourcesToClose() {
        return delegatingRecoveryHandler.getResources();
    }

    private Consumer<Exception> consumerForCleanupOnFailure(Consumer<Exception> onFailure) {
        Consumer<Exception> cleanUpConsumer = (e) -> {
            try {
                cleanupChildShardDirectories();
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] failed to cleanup child shard directories failure ([{}])",
                        sourceShard.shardId(),
                        e.getMessage()
                    ),
                    inner
                );
            }
        };
        return cleanUpConsumer.andThen(onFailure);
    }

    @Override
    protected void innerRecoveryToTarget(ActionListener<RecoveryResponse> listener, Consumer<Exception> onFailure) throws IOException {
//        onFailure = consumerForCleanupOnFailure(onFailure);
        // Clean up shard directories if previous shard closures failed.
        cleanupChildShardDirectories();

        List<Releasable> delayedStaleCommitDeleteOps = sourceShard.delayStaleCommitDeletions();
        resources.addAll(delayedStaleCommitDeleteOps);
        GatedCloseable<Long> translogRetentionLock = sourceShard.acquireRetentionLockWithMinGen();
        resources.add(translogRetentionLock);
        // Make sure that all operations before acquired translog generation are present in the last commit.
        // In remote store replication mode refreshed but not flushed ops are also trimmed from translog and hence,
        // a flush is required to ensure that all operations before the acquired translog are present in the local commit.
        // Also, a refresh is done as part of flush and therefore, we can expect commit to be present in remote store
        // as well.
        sourceShard.flush(new FlushRequest().waitIfOngoing(true).force(true));

        Releasable releaseStore = acquireStore(sourceShard.store());
        resources.add(releaseStore);
        GatedCloseable<IndexCommit> lastCommit = acquireLastCommit(sourceShard,false);
        resources.add(lastCommit);

        Tuple<String, RemoteSegmentMetadata> fetchedMetadataTuple = null;
        if (sourceShard.remoteStore() != null) {
            fetchedMetadataTuple = sourceShard.getMetadataContentForCommit(
                sourceShard.getOperationPrimaryTerm(),
                lastCommit.get().getGeneration());
            ensureMetadataHasAllSegmentsFromCommit(lastCommit.get(), fetchedMetadataTuple.v2());
        }
        splitCommitMetadata.set(new SplitCommitMetadata(translogRetentionLock.get(), fetchedMetadataTuple));

        final StepListener<SendFileResult> sendFileStep = new StepListener<>();
        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
        final StepListener<List<SendSnapshotResult>> sendSnapshotStep = new StepListener<>();

        postSendFileComplete(sendFileStep, lastCommit, releaseStore, delayedStaleCommitDeleteOps);
        long startingSeqNo = Long.parseLong(lastCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
        logger.info("Docs in commit " + (startingSeqNo));
        assert Transports.assertNotTransportThread(this + "[phase1]");
        phase1(lastCommit.get(), startingSeqNo, () -> 0, sendFileStep, true);

        prepareEngine(sendFileStep, prepareEngineStep, RecoveryState.Translog.UNKNOWN, onFailure);

        prepareEngineStep.whenComplete(prepareEngineTime -> {
            logger.info("prepareEngineStep completed");
            assert Transports.assertNotTransportThread(this + "[phase2]");
            initiateTracking();

            final long endingSeqNo = sourceShard.seqNoStats().getMaxSeqNo();
            final Translog.Snapshot phase2Snapshot;
            if (startingSeqNo > endingSeqNo) {
                phase2Snapshot = new EmptySnapshot();
            } else {
                phase2Snapshot = sourceShard.getHistoryOperationsFromTranslog(startingSeqNo, endingSeqNo);
            }

            resources.add(phase2Snapshot);
            translogRetentionLock.close();
            logger.info("snapshot translog for recovery; current size is [{}]", phase2Snapshot.totalOperations());

            final long mappingVersionOnPrimary = sourceShard.indexSettings().getIndexMetadata().getMappingVersion();
            phase2(
                startingSeqNo,
                endingSeqNo,
                phase2Snapshot,
                sourceShard.getMaxSeenAutoIdTimestamp(),
                sourceShard.getMaxSeqNoOfUpdatesOrDeletes(),
                sourceShard.getRetentionLeases(),
                mappingVersionOnPrimary,
                sendSnapshotStep
            );
        }, onFailure);

        StepListener<Void> finalizeStep = new StepListener<>();
        sendSnapshotStep.whenComplete(r -> logger.info("Send snapshot step completed."), onFailure);
        finalizeStep.whenComplete(r -> {
            logger.info("Finalize step completed.");
            cleanUpMaybeRemoteOnFinalize();
        }, onFailure);
        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStepWithEmptyResult(), prepareEngineStep, finalizeStep, onFailure);
    }

    private void ensureMetadataHasAllSegmentsFromCommit(IndexCommit indexCommit, RemoteSegmentMetadata metadata) throws IOException {
        List<String> missingFiles = new ArrayList<>();
        for (String file : indexCommit.getFileNames()) {
            if (metadata.getMetadata().containsKey(file) == false) {
                missingFiles.add(file);
            }
        }

        if (missingFiles.isEmpty() == false) {
            throw new IllegalStateException("Missing segments in remote segments metadata. Missing files ["
                + missingFiles + "] for commit generation [" + indexCommit.getGeneration() + "]");
        }
    }

    protected void postSendFileComplete(
        StepListener<SendFileResult> sendFileStep,
        GatedCloseable<IndexCommit> wrappedSafeCommit,
        Releasable releaseStore,
        List<Releasable> delayedStaleCommitOps
    ) {
        sendFileStep.whenComplete(r -> {
            logger.info("sendFileStep completed");
            delayedStaleCommitOps.forEach(Releasable::close);
            IOUtils.close(wrappedSafeCommit, releaseStore);
        }, e -> {
            try {
                IOUtils.close(wrappedSafeCommit, releaseStore);
                delayedStaleCommitOps.forEach(Releasable::close);
            } catch (final IOException ex) {
                logger.warn("releasing snapshot caused exception", ex);
            }
        });
    }

    @Override
    public int countNumberOfHistoryOperations(long startingSeqNo) throws IOException {
        return delegatingRecoveryHandler.countNumberOfHistoryOperations(startingSeqNo);
    }

    @Override
    public Closeable acquireRetentionLock() {
        return delegatingRecoveryHandler.acquireRetentionLock();
    }

    public void prepareEngine(StepListener<SendFileResult> sendFileStep,
                              StepListener<TimeValue> prepareEngineStep,
                              int totalTranslogOps,
                              Consumer<Exception> onFailure) {
        sendFileStep.whenComplete(r -> {
            logger.info("sendFileStep completed");
            assert Transports.assertNotTransportThread(this + "[prepareTargetForTranslog]");
            // For a sequence based recovery, the target can keep its local translog
            prepareTargetForTranslog(totalTranslogOps, prepareEngineStep);
        }, onFailure);
    }

    @Override
    public Translog.Snapshot phase2Snapshot(long startingSeqNo, String recoveryName) throws IOException {
        return null;
    }

    private static class EmptySnapshot implements Translog.Snapshot {
        @Override
        public int totalOperations() {
            return 0;
        }

        @Override
        public Translog.Operation next() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    @Override
    public boolean shouldSkipCreateRetentionLeaseStep() {
        return delegatingRecoveryHandler.shouldSkipCreateRetentionLeaseStep();
    }

    @Override
    protected void updateGlobalCheckpointForShard(long globalCheckpoint) {
        childShardsAllocationIds.forEach(allocationID -> {
            RunUnderPrimaryPermit.run(
                () -> shard.updateGlobalCheckpointForShard(allocationID, globalCheckpoint),
                shardId + " updating " + allocationID + "'s global checkpoint",
                shard,
                cancellableThreads,
                logger
            );
        });
    }

    @Override
    protected void relocateShard(Runnable forceSegRepRunnable) throws InterruptedException {
        shard.relocated(childShardsAllocationIds, recoveryTarget::handoffPrimaryContext, forceSegRepRunnable);
        recoveryTarget.flushOnAllChildShards();
    }

    public void cleanupChildShardDirectories() throws IOException {
        recoveryTarget.cleanShardDirectoriesForTargets();
    }

    protected void sendFiles(Store store, StoreFileMetadata[] files, IntSupplier translogOps,
                             ActionListener<Void> listener, IndexCommit snapshot) {

        try {
            long localCheckpoint = Long.parseLong(snapshot.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            long maxSeqNo = Long.parseLong(snapshot.getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            long maxUnsafeAutoIdTimestamp = Long.parseLong(snapshot.getUserData().get(
                Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID));
            recoveryTarget.receiveFilesAndSplit(store, files, localCheckpoint, maxSeqNo, splitCommitMetadata.get(),
                maxUnsafeAutoIdTimestamp);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void cleanFiles(
        Store store,
        Store.MetadataSnapshot sourceMetadata,
        IntSupplier translogOps,
        long globalCheckpoint,
        ActionListener<Void> listener
    ) {
        recoveryTarget.cleanFiles(translogOps.getAsInt(), globalCheckpoint, sourceMetadata, listener);
    }

    private void initiateTracking() {
        cancellableThreads.checkForCancel();
        List<String> allocationIDs = new ArrayList<>();
        recoveryContexts.forEach(context -> allocationIDs.add(context.getIndexShard()
            .routingEntry().allocationId().getId()));

        RunUnderPrimaryPermit.run(
            () -> shard.initiateTrackingOfChildShards(allocationIDs),
            shardId + " initiating tracking of " + allocationIDs,
            shard,
            cancellableThreads,
            logger
        );
    }

    private StepListener<SendFileResult> sendFileStepWithEmptyResult() {
        StepListener<SendFileResult> sendFileStep = new StepListener<>();
        sendFileStep.onResponse(new SendFileResult(
                Collections.emptyList(),
                Collections.emptyList(),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                0,
                TimeValue.ZERO
            )
        );

        return sendFileStep;
    }

    @Override
    protected OperationBatchSender createSender(
        final long startingSeqNo,
        final long endingSeqNo,
        final Translog.Snapshot snapshot,
        final long maxSeenAutoIdTimestamp,
        final long maxSeqNoOfUpdatesOrDeletes,
        final RetentionLeases retentionLeases,
        final long mappingVersion,
        StepListener<Void> sendListener
    ) {
        return new AllShardsOperationBatchSender(startingSeqNo, endingSeqNo, snapshot, maxSeenAutoIdTimestamp,
            maxSeqNoOfUpdatesOrDeletes, retentionLeases, mappingVersion, sendListener);
    }

    @Override
    protected List<SendSnapshotResult> createSnapshotResult(OperationBatchSender sender, int totalSentOps,
                                                            TimeValue tookTime) {
        assert sender instanceof AllShardsOperationBatchSender;
        AllShardsOperationBatchSender allShardsSender = (AllShardsOperationBatchSender) sender;
        List<SendSnapshotResult> sendSnapshotResults = new ArrayList<>(childShardsAllocationIds.size());
        allShardsSender.targetLocalCheckpoints.forEach((allocationId, checkpoint) -> {
            sendSnapshotResults.add(new SendSnapshotResult(checkpoint.get(), totalSentOps,
                tookTime, allocationId));
        });
        return sendSnapshotResults;
    }

    protected class AllShardsOperationBatchSender extends OperationBatchSender {
        private final Map<String, AtomicLong> targetLocalCheckpoints = new HashMap<>();

        protected AllShardsOperationBatchSender(
            long startingSeqNo, long endingSeqNo, Translog.Snapshot snapshot,
            long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes,
            RetentionLeases retentionLeases, long mappingVersion, ActionListener<Void> listener) {
            super(startingSeqNo, endingSeqNo, snapshot, maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes, retentionLeases, mappingVersion, listener);

            childShardsAllocationIds.forEach(childShardsAllocationId -> {
                targetLocalCheckpoints.put(childShardsAllocationId, new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED));
            });
        }

        @Override
        protected void executeChunkRequest(OperationChunkRequest request, ActionListener<Void> listener) {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperationsOnShards(
                request.getOperations(),
                snapshot.totalOperations(),
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersion,
                ActionListener.delegateFailure(listener, (l, allocationCheckpoints) -> {
                    allocationCheckpoints.forEach(allocationCheckpoint -> {
                        targetLocalCheckpoints.get(allocationCheckpoint.allocationId).updateAndGet(curr ->
                            SequenceNumbers.max(curr, allocationCheckpoint.checkpoint));
                    });
                    l.onResponse(null);
                })
            );
        }
    }

    private void cleanUpMaybeRemoteOnFinalize() {
        Store remoteStore = sourceShard.remoteStore();
        if (remoteStore != null) {
            try(Releasable releasable = acquireStore(sourceShard.remoteStore())) {
                resources.add(releasable);
                sourceShard.cleanUpRemoteDirectories();
            } catch (IOException e) {
                logger.error("Failed to cleanup source shard remote directory", e);
            }
        }
    }
}
