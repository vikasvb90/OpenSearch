/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

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
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.StoreRecovery;
import org.opensearch.index.store.Store;
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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class InPlaceShardSplitRecoverySourceHandler extends RecoverySourceHandler {
    private final List<InPlaceShardRecoveryContext> recoveryContexts;
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

        this.recoveryContexts = recoveryContexts;
        this.sourceShard = sourceShard;
        this.delegatingRecoveryHandler = delegatingRecoveryHandler;
        this.shardIds = shardIds;
        this.childShardsAllocationIds = childShardsAllocationIds;
    }

    public List<Closeable> getAdditionalResourcesToClose() {
        return delegatingRecoveryHandler.getResources();
    }

    @Override
    protected void innerRecoveryToTarget(ActionListener<RecoveryResponse> listener, Consumer<Exception> onFailure) throws IOException {
        // Clean up shard directories if previous shard closures failed.
        cleanupChildShardDirectories();

        recoveryContexts.forEach(context -> context.getIndexShard().preRecovery());
        recoveryContexts.forEach(context -> context.getIndexShard().prepareForIndexRecovery());

        cancellableThreads.checkForCancel();
        final Closeable retentionLock = delegatingRecoveryHandler.acquireRetentionLock();
        if (retentionLock != null) {
            resources.add(retentionLock);
        }

        long startingSeqNo;
        try(GatedCloseable<IndexCommit> wrappedSafeCommit = acquireSafeCommit(sourceShard);
            Releasable releaseStore = acquireStore(sourceShard.store())) {
            resources.add(wrappedSafeCommit);
            resources.add(releaseStore);
            addLuceneIndices(wrappedSafeCommit);
            startingSeqNo = Long.parseLong(wrappedSafeCommit.get().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1L;
        }

        // We don't need to clone retention leases here even in DocRep case because we always restart the recovery
        // of child shards from the beginning after any failure.

        final StepListener<TimeValue> prepareEngineStep = new StepListener<>();
        final StepListener<SendSnapshotResult> sendSnapshotStep = new StepListener<>();
        prepareTargetForTranslog(countNumberOfHistoryOperations(startingSeqNo), prepareEngineStep);

        // We don't need to rely on recovery target callbacks in listener because at present executions in targets are
        // synchronous. But we still handle callbacks in listener so that any future changes to make targets async do
        // not require changes here and shard split recoveries continue to work seamlessly.
        prepareEngineStep.whenComplete(prepareEngineTime -> {
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

            executePhase2Snapshot(startingSeqNo, endingSeqNo, phase2Snapshot, sendSnapshotStep, sourceShard);
        }, onFailure);

        finalizeStepAndCompleteFuture(startingSeqNo, sendSnapshotStep, sendFileStepWithEmptyResult(), prepareEngineStep, onFailure);
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
    public void executePhase2Snapshot(long startingSeqNo, long endingSeqNo, Translog.Snapshot phase2Snapshot,
                                      StepListener<SendSnapshotResult> sendSnapshotStep, IndexShard shard) throws IOException {
        delegatingRecoveryHandler.executePhase2Snapshot(startingSeqNo, endingSeqNo, phase2Snapshot, sendSnapshotStep, shard);
    }

    @Override
    protected void markAllocationIdAsInSync(long targetLocalCheckpoint) {
        /*
         * Before marking the shard as in-sync we acquire an operation permit. We do this so that there is a barrier between marking a
         * shard as in-sync and relocating a shard. If we acquire the permit then no relocation handoff can complete before we are done
         * marking the shard as in-sync. If the relocation handoff holds all the permits then after the handoff completes and we acquire
         * the permit then the state of the shard will be relocated and this recovery will fail.
         */
        childShardsAllocationIds.forEach(allocationID -> {
            RunUnderPrimaryPermit.run(
                () -> shard.markAllocationIdAsInSync(allocationID, targetLocalCheckpoint),
                shardId + " marking " + allocationID + " as in sync",
                shard,
                cancellableThreads,
                logger
            );
        });
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

    private void cleanupChildShardDirectories() throws IOException {
        for (InPlaceShardRecoveryContext context : recoveryContexts) {
            cancellableThreads.checkForCancel();
            try(Releasable releasable = acquireStore(context.getIndexShard().store())) {
                resources.add(releasable);
                Store store = context.getIndexShard().store();
                Directory storeDirectory = store.directory();
                for (String file : storeDirectory.listAll()) {
                    storeDirectory.deleteFile(file);
                }
            }
        }
    }

    private void addLuceneIndices(GatedCloseable<IndexCommit> wrappedSafeCommit) throws IOException {
        long maxSeqNo = Long.parseLong(wrappedSafeCommit.get().getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        long maxUnsafeAutoIdTimestamp = Long.parseLong(wrappedSafeCommit.get().getUserData().get(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID));
        resources.add(wrappedSafeCommit);
        for (InPlaceShardRecoveryContext recoveryContext : recoveryContexts) {
            cancellableThreads.checkForCancel();
            StoreRecovery.addIndices(
                recoveryContext.getRecoveryState().getIndex(),
                recoveryContext.getIndexShard().store().directory(),
                sourceShard.getIndexSort(),
                new Directory[]{sourceShard.store().directory()},
                maxSeqNo,
                maxUnsafeAutoIdTimestamp,
                sourceShard.indexSettings().getIndexMetadata(),
                recoveryContext.getIndexShard().shardId().id(),
                true,
                sourceShard.mapperService().hasNested()
            );
        }
    }

    private void initiateTracking() {
        cancellableThreads.checkForCancel();
        List<String> allocationIDs = new ArrayList<>();
        recoveryContexts.forEach(context -> allocationIDs.add(context.getIndexShard().getAllocationId()));

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
}
