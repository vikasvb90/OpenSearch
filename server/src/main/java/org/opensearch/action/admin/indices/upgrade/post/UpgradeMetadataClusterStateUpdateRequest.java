/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.upgrade.post;

import org.opensearch.cluster.ack.ClusterStateUpdateRequest;

import java.util.List;

public class UpgradeMetadataClusterStateUpdateRequest extends ClusterStateUpdateRequest<UpgradeSettingsClusterStateUpdateRequest> {
    private final List<String> indices;

    public UpgradeMetadataClusterStateUpdateRequest(List<String> indices) {
        this.indices = indices;
    }

    /**
     * Returns the list of indices to be upgraded
     */
    public List<String> getIndices() {
        return indices;
    }
}
