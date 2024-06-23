/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.inplacesplit;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;

/**
 * Container for metadata of commit acquired during shard split
 */
public class SplitCommitMetadata {
    private final Long translogGen;
    private final Tuple<String, RemoteSegmentMetadata> metadataTuple;

    public SplitCommitMetadata(Long translogGen, Tuple<String, RemoteSegmentMetadata> metadataTuple) {
        this.translogGen = translogGen;
        this.metadataTuple = metadataTuple;
    }

    public Long getTranslogGen() {
        return translogGen;
    }

    public Tuple<String, RemoteSegmentMetadata> getMetadataTuple() {
        return metadataTuple;
    }
}
