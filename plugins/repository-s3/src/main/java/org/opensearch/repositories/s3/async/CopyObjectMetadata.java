/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import java.util.Map;

/**
 * Copy object metadata of an object to be copied from a remote directory to another remote directory.
 */
public class CopyObjectMetadata {
    private Integer partCount;
    private Map<String, String> metadata;
    private Map<Integer, Long> partSizes;

    public Integer getPartCount() {
        return partCount;
    }

    public void setPartCount(Integer partCount) {
        this.partCount = partCount;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Map<Integer, Long> getPartSizes() {
        return partSizes;
    }

    public void setPartSizes(Map<Integer, Long> partSizes) {
        this.partSizes = partSizes;
    }
}
