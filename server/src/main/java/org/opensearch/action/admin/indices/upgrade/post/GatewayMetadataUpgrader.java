/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.upgrade.post;

public class GatewayMetadataUpgrader implements Runnable {
    private final Runnable gatewayMetadataUpgrader;

    public GatewayMetadataUpgrader(Runnable gatewayMetadataUpgrader) {
        this.gatewayMetadataUpgrader = gatewayMetadataUpgrader;
    }

    @Override
    public void run() {
        gatewayMetadataUpgrader.run();
    }
}
