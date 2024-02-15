/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationFailedException;

import java.util.Arrays;
import java.util.List;

/**
 * Shard recovery exception in-place shard recovery.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class InPlaceShardsRecoveryFailedException extends ReplicationFailedException {

    public InPlaceShardsRecoveryFailedException(ShardId sourceShardId) {
        super("Split of shard : " + sourceShardId + " failed.");
    }

}
