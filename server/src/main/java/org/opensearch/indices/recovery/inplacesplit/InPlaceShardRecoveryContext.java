/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoveryState;

/**
 * Shard recovery context of a child shard required for in-place shard recovery.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class InPlaceShardRecoveryContext {

    private final RecoveryState recoveryState;
    private final IndexShard indexShard;
    private final IndexShard sourceShard;

    /**
     * Constructor for recovery context.
     */
    public InPlaceShardRecoveryContext(RecoveryState recoveryState, IndexShard indexShard, IndexShard sourceShard) {
        this.recoveryState = recoveryState;
        this.indexShard = indexShard;
        this.sourceShard = sourceShard;
    }

    public RecoveryState getRecoveryState() {
        return recoveryState;
    }

    public IndexShard getIndexShard() {
        return indexShard;
    }

    public IndexShard getSourceShard() {
        return sourceShard;
    }
}
