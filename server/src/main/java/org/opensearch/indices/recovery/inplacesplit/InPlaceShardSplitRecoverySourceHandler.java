/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.opensearch.action.StepListener;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.recovery.RecoveryResponse;
import org.opensearch.indices.recovery.RecoverySourceHandler;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
    private final String IN_PLACE_SHARD_SPLIT_RECOVERY_NAME = "in-place-shard-split-recovery";
    private final RecoverySourceHandler delegatingRecoveryHandler;
    private final List<ShardId> shardIds;
    private final Set<String> childShardsAllocationIds;

    public InPlaceShardSplitRecoverySourceHandler(
        IndexShard sourceShard,
        ThreadPool threadPool,
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
            threadPool, request, fileChunkSizeInBytes, maxConcurrentFileChunks,
            maxConcurrentOperations, true, cancellableThreads);

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
        onFailure = consumerForCleanupOnFailure(onFailure);
        // Clean up shard directories if previous shard closures failed.
        cleanupChildShardDirectories();

        cancellableThreads.checkForCancel();
        final Closeable retentionLock = delegatingRecoveryHandler.acquireRetentionLock();
        if (retentionLock != null) {
            resources.add(retentionLock);
        }

        final StepListener<SendFileResult> sendFileStep = new StepListener<>();
        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
        final StepListener<List<SendSnapshotResult>> sendSnapshotStep = new StepListener<>();

        final GatedCloseable<IndexCommit> wrappedSafeCommit = acquireSafeCommit(shard);
        resources.add(wrappedSafeCommit);
        Releasable releaseStore = acquireStore(sourceShard.store());
        resources.add(releaseStore);

        long startingSeqNo = Long.parseLong(wrappedSafeCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
        int estimatedOps = countNumberOfHistoryOperations(startingSeqNo);
        onSendFileStepComplete(sendFileStep, wrappedSafeCommit, releaseStore);

        assert Transports.assertNotTransportThread(this + "[phase1]");
        // We don't need to clone retention leases here even in DocRep case because we always restart the recovery
        // of child shards from the beginning after any failure.
        phase1(wrappedSafeCommit.get(), startingSeqNo, () -> estimatedOps, sendFileStep, true);

        sendFileStep.whenComplete(r -> {
            logger.debug("sendFileStep completed");
            assert Transports.assertNotTransportThread(this + "[prepareTargetForTranslog]");
            // For a sequence based recovery, the target can keep its local translog
            prepareTargetForTranslog(estimatedOps, prepareEngineStep);
        }, onFailure);

        // We don't need to rely on recovery target callbacks in listener because at present executions in targets are
        // synchronous. But we still handle callbacks in listener so that any future changes to make targets async do
        // not require changes here and shard split recoveries continue to work seamlessly.
        prepareEngineStep.whenComplete(prepareEngineTime -> {
            // Do a refresh here. In case of remote replication, refresh will upload segments to remote child directories.
            recoveryTarget.refresh();

            logger.debug("prepareEngineStep completed");
            assert Transports.assertNotTransportThread(this + "[phase2]");
            initiateTracking();

            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
            final Translog.Snapshot phase2Snapshot = phase2Snapshot(startingSeqNo, IN_PLACE_SHARD_SPLIT_RECOVERY_NAME);
            if (phase2Snapshot != null) {
                resources.add(phase2Snapshot);
                if (logger.isTraceEnabled()) {
                    logger.trace("snapshot translog for recovery; current size is [{}]", countNumberOfHistoryOperations(startingSeqNo));
                }
            }
            if (retentionLock != null) {
                retentionLock.close();
            }
            if (phase2Snapshot != null) {
                final long mappingVersionOnPrimary = shard.indexSettings().getIndexMetadata().getMappingVersion();
                phase2(
                    startingSeqNo,
                    endingSeqNo,
                    phase2Snapshot,
                    shard.getMaxSeenAutoIdTimestamp(),
                    shard.getMaxSeqNoOfUpdatesOrDeletes(),
                    shard.getRetentionLeases(),
                    mappingVersionOnPrimary,
                    sendSnapshotStep
                );
            }

        }, onFailure);

        StepListener<Void> finalizeStep = new StepListener<>();
        finalizeStep.whenComplete(r -> cleanUpMaybeRemoteOnFinalize(), onFailure);
        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStepWithEmptyResult(), prepareEngineStep, finalizeStep, onFailure);
    }

    @Override
    public int countNumberOfHistoryOperations(long startingSeqNo) throws IOException {
        return delegatingRecoveryHandler.countNumberOfHistoryOperations(startingSeqNo);
    }

    @Override
    public Closeable acquireRetentionLock() {
        return delegatingRecoveryHandler.acquireRetentionLock();
    }

    @Override
    public Translog.Snapshot phase2Snapshot(long startingSeqNo, String recoveryName) throws IOException {
        return delegatingRecoveryHandler.phase2Snapshot(startingSeqNo, recoveryName);
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
    }

    public void cleanupChildShardDirectories() throws IOException {
        recoveryTarget.cleanShardDirectoriesForTargets();
    }

    protected void sendFiles(Store store, StoreFileMetadata[] files, IntSupplier translogOps,
                             ActionListener<Void> listener, IndexCommit snapshot) {

        try {
            long maxSeqNo = Long.parseLong(snapshot.getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            long maxUnsafeAutoIdTimestamp = Long.parseLong(snapshot.getUserData().get(
                Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID));
            recoveryTarget.receiveFilesAndSplit(store, files, maxSeqNo, maxUnsafeAutoIdTimestamp);
            listener.onResponse(null);
        } catch (IOException e) {
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
            () -> shard.initiateTrackingOfBulkShards(allocationIDs),
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
                Directory storeDirectory = remoteStore.directory();
                for (String file : storeDirectory.listAll()) {
                    storeDirectory.deleteFile(file);
                }
            } catch (IOException e) {
                logger.error("Failed to cleanup source shard remote directory", e);
            }
        }
    }
}
