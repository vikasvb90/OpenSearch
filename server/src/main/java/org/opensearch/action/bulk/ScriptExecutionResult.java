/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Holds the result of script execution on a shard.
 * Contains items that should be processed locally and items that need to be redirected
 * to the coordinator for re-routing to the correct shard.
 *
 * @opensearch.internal
 */
public class ScriptExecutionResult {

    private final List<BulkItemRequest> itemsToProcess;
    private final List<BulkItemRequest> itemsToRedirect;

    /**
     * Creates a new ScriptExecutionResult with the given items.
     *
     * @param itemsToProcess  List of items that should be processed locally on this shard
     * @param itemsToRedirect List of items that need to be redirected to the coordinator
     */
    public ScriptExecutionResult(List<BulkItemRequest> itemsToProcess, List<BulkItemRequest> itemsToRedirect) {
        this.itemsToProcess = Objects.requireNonNull(itemsToProcess, "itemsToProcess cannot be null");
        this.itemsToRedirect = Objects.requireNonNull(itemsToRedirect, "itemsToRedirect cannot be null");
    }

    /**
     * Returns the list of items that should be processed locally on this shard.
     *
     * @return unmodifiable list of items to process
     */
    public List<BulkItemRequest> getItemsToProcess() {
        return Collections.unmodifiableList(itemsToProcess);
    }

    /**
     * Returns the list of items that need to be redirected to the coordinator.
     *
     * @return unmodifiable list of items to redirect
     */
    public List<BulkItemRequest> getItemsToRedirect() {
        return Collections.unmodifiableList(itemsToRedirect);
    }

    /**
     * Checks if there are any items that need to be redirected.
     *
     * @return true if there are items to redirect, false otherwise
     */
    public boolean hasItemsToRedirect() {
        return itemsToRedirect != null && !itemsToRedirect.isEmpty();
    }
}
