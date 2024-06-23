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
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoveryFailedException;
import org.opensearch.indices.recovery.RecoveryResponse;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.util.List;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;

public class InPlaceShardSplitResponseHandler implements ActionListener<RecoveryResponse> {
    private static final Logger logger = LogManager.getLogger(InPlaceShardSplitResponseHandler.class);

    private final ReplicationListener replicationListener;
    private final StartRecoveryRequest request;
    private final long recoveryId;
    private final List<ReplicationTimer> timers;
    private final InPlaceShardSplitRecoveryService.OngoingRecoveries ongoingRecoveries;
    private final IndexShard sourceShard;

    public InPlaceShardSplitResponseHandler(final ReplicationListener replicationListener, StartRecoveryRequest request,
                                            final List<ReplicationTimer> timers,
                                            InPlaceShardSplitRecoveryService.OngoingRecoveries ongoingRecoveries,
                                            IndexShard sourceShard) {
        this.replicationListener = replicationListener;
        this.request = request;
        this.recoveryId = request.recoveryId();
        this.timers = timers;
        this.ongoingRecoveries = ongoingRecoveries;
        this.sourceShard = sourceShard;
    }

    @Override
    public void onResponse(RecoveryResponse recoveryResponse) {
        ongoingRecoveries.markAsDone(sourceShard);
        long maxRecoveryTime = Long.MIN_VALUE;
        for (ReplicationTimer timer : timers) {
            maxRecoveryTime = Math.max(timer.time(), maxRecoveryTime);
        }

        final TimeValue recoveryTime = new TimeValue(maxRecoveryTime);
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append('[')
                .append(request.shardId().getIndex().getName())
                .append(']')
                .append('[')
                .append(request.shardId().id())
                .append("] ");
            sb.append("in-place shard split recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime).append("]\n");
            sb.append("   phase1: shard split ")
                .append(", took [")
                .append(timeValueMillis(recoveryResponse.getPhase1Time()))
                .append(']')
                .append("\n");
            sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.getStartTime())).append("]\n");
            sb.append("         : recovered [")
                .append(recoveryResponse.getPhase2Operations())
                .append("]")
                .append(" transaction log operations")
                .append(", took [")
                .append(timeValueMillis(recoveryResponse.getPhase2Time()))
                .append("]")
                .append("\n");
            logger.trace("{}", sb);
        } else {
            logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(), recoveryTime);
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof AlreadyClosedException) {
            ongoingRecoveries.fail(sourceShard, new RecoveryFailedException(request, "source shard is closed", e.getCause()), false);
            return;
        }
        logger.error("Shard split failed for source shard " + sourceShard.shardId(), e);
        ongoingRecoveries.fail(sourceShard, new RecoveryFailedException(request, e.getCause()), true);
    }
}
