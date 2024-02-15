/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.localsplit;

import org.opensearch.action.ActionType;

public class LocalShardSplitAction extends ActionType<LocalShardSplitResponse> {

    public static final LocalShardSplitAction INSTANCE = new LocalShardSplitAction();
    public static final String NAME = "indices:admin/local_shard_split";

    private LocalShardSplitAction() {
        super(NAME, LocalShardSplitResponse::new);
    }
}
