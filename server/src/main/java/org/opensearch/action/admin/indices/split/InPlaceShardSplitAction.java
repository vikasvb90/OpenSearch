/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.ActionType;

public class InPlaceShardSplitAction extends ActionType<InPlaceShardSplitResponse> {

    public static final InPlaceShardSplitAction INSTANCE = new InPlaceShardSplitAction();
    public static final String NAME = "indices:admin/in_place_shard_split";

    private InPlaceShardSplitAction() {
        super(NAME, InPlaceShardSplitResponse::new);
    }
}
