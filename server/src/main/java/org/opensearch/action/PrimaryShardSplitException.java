/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.OpenSearchException;

/**
 * Exception thrown when waiting writes fail due to primary shard getting split.
 *
 * @opensearch.internal
 */
public class PrimaryShardSplitException extends OpenSearchException {
    public PrimaryShardSplitException(String msg) {
        super(msg);
    }
}
