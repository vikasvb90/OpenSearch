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
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryResponse;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoverySourceHandler;
import org.opensearch.indices.recovery.RecoverySourceHandlerFactory;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.indices.replication.SegmentReplicationSourceFactory;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InPlaceShardSplitRecoveryService extends AbstractLifecycleComponent implements IndexEventListener, ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(InPlaceShardSplitRecoveryService.class);

    private final OngoingRecoveries ongoingRecoveries;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;
    private final SegmentReplicationSourceFactory segRepFactory;

    @Inject
    public InPlaceShardSplitRecoveryService(IndicesService indicesService, RecoverySettings recoverySettings,
                                            SegmentReplicationSourceFactory segRepFactory) {
        this.ongoingRecoveries = new OngoingRecoveries();
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.segRepFactory = segRepFactory;
    }

    @Override
    protected void doStart() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        final ClusterService clusterService = indicesService.clusterService();
        if (DiscoveryNode.isDataNode(clusterService.getSettings())) {
            ongoingRecoveries.awaitEmpty();
            indicesService.clusterService().removeListener(this);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard, "shard is closed");
        }
    }

    /**
     * Cancels in-place shard recovery for a shard whose replica on another node has been promoted as primary.
     */
    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        if (indexShard != null && oldRouting.primary() == false && newRouting.primary()) {
            ongoingRecoveries.cancel(indexShard, "Relocating primary shard.");
        }
    }

    @Override
    protected void doClose() throws IOException {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {}

    public void addAndStartRecovery(List<InPlaceShardRecoveryContext> recoveryContexts,
                                    DiscoveryNode node,
                                    IndexShard sourceShard,
                                    InPlaceShardSplitRecoveryListener replicationListener,
                                    List<ShardId> shardIds,
                                    StartRecoveryRequest request) {
        Set<String> childShardAllocationIds = new HashSet<>();
        recoveryContexts.forEach(context -> childShardAllocationIds.add(context.getIndexShard()
            .routingEntry().allocationId().getId()));

        List<ReplicationTimer> timers = new ArrayList<>();
        recoveryContexts.forEach(context -> timers.add(context.getRecoveryState().getTimer()));
        ActionListener<RecoveryResponse> recoveryResponseListener = new InPlaceShardSplitResponseHandler(
            replicationListener, request, timers, ongoingRecoveries, sourceShard);

        InPlaceShardSplitRecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(sourceShard, node,
            recoveryContexts, request, shardIds, childShardAllocationIds, replicationListener);
        logger.trace(
            "[{}] starting in-place recovery from [{}]",
            sourceShard.shardId().getIndex().getName(),
            sourceShard.shardId().id()
        );

        handler.recoverToTarget(recoveryResponseListener);
    }

    public class OngoingRecoveries {
        private final Map<ShardId, Recovery> recoveries = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        private class Recovery {
            private final InPlaceShardSplitRecoveryTargetHandler targetHandler;
            private final InPlaceShardSplitRecoverySourceHandler sourceHandler;
            private final InPlaceShardSplitRecoveryListener replicationListener;

            public Recovery(InPlaceShardSplitRecoveryTargetHandler targetHandler,
                            InPlaceShardSplitRecoverySourceHandler sourceHandler,
                            InPlaceShardSplitRecoveryListener replicationListener) {
                this.targetHandler = targetHandler;
                this.sourceHandler = sourceHandler;
                this.replicationListener = replicationListener;
            }
        }

        synchronized InPlaceShardSplitRecoverySourceHandler addNewRecovery(
            IndexShard sourceShard, DiscoveryNode node, List<InPlaceShardRecoveryContext> recoveryContexts,
            StartRecoveryRequest request, List<ShardId> shardIds, Set<String> childShardsAllocationIds,
            InPlaceShardSplitRecoveryListener replicationListener) {
            assert lifecycle.started();
            if (recoveries.containsKey(sourceShard.shardId())) {
                throw new IllegalStateException("In-place shard recovery from shard " + sourceShard.shardId() + "  already already in progress");
            }
            CancellableThreads cancellableThreads = new CancellableThreads();
            List<IndexShard> targetShards = new ArrayList<>();
            recoveryContexts.forEach(context -> targetShards.add(context.getIndexShard()));

            InPlaceShardSplitRecoveryTargetHandler targetHandler = new InPlaceShardSplitRecoveryTargetHandler(targetShards,
                node, cancellableThreads, recoveryContexts, childShardsAllocationIds ,sourceShard, segRepFactory);
            RecoverySourceHandler delegatingRecoveryHandler = RecoverySourceHandlerFactory.create(
                sourceShard, targetHandler, request,
                recoverySettings, true, cancellableThreads);

            InPlaceShardSplitRecoverySourceHandler sourceHandler = new InPlaceShardSplitRecoverySourceHandler(sourceShard,
                sourceShard.getThreadPool(), targetHandler, delegatingRecoveryHandler, request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(), recoverySettings.getMaxConcurrentOperations(),
                cancellableThreads, recoveryContexts, shardIds, childShardsAllocationIds);

            recoveries.put(sourceShard.shardId(), new Recovery(targetHandler, sourceHandler, replicationListener));
            sourceShard.recoveryStats().incCurrentAsSource();
            return sourceHandler;
        }

        synchronized void remove(InPlaceShardSplitRecoverySourceHandler sourceHandler) {
            sourceHandler.getSourceShard().recoveryStats().decCurrentAsSource();
            if (recoveries.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        public synchronized void markAsDone(IndexShard sourceShard) {
            Recovery removed = recoveries.remove(sourceShard.shardId());
            if (removed != null) {
                assert sourceShard.routingEntry().splitting();
                remove(removed.sourceHandler);
                removed.targetHandler.onDone();
                removed.replicationListener.onDone(null);
            }
        }

        public synchronized void fail(IndexShard sourceShard, ReplicationFailedException ex, boolean sendShardFailure) {
            Recovery removed = recoveries.remove(sourceShard.shardId());
            if (removed != null) {
                remove(removed.sourceHandler);
                removed.replicationListener.onFailure(null, ex, sendShardFailure);
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            try {
                ShardId sourceShardId = getSplittingSourceShardId(shard);
                if (sourceShardId != null && recoveries.containsKey(sourceShardId)) {
                    recoveries.get(sourceShardId).sourceHandler.cancel(reason);
                }
            } catch (Exception ex) {
                throw new OpenSearchException(ex);
            } finally {
                shard.recoveryStats().decCurrentAsSource();
            }
        }

        private ShardId getSplittingSourceShardId(IndexShard shard) {
            if (shard.routingEntry().splitting()) {
                return shard.shardId();
            } else if (shard.routingEntry().isSplitTarget()) {
                return shard.routingEntry().getSplittingShardId();
            }
            return null;
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (recoveries.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

    }

}
