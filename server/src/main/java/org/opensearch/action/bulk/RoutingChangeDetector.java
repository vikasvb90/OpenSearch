/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;

import java.util.Objects;

/**
 * Helper class to detect and handle routing key changes after script execution.
 * This class provides utilities to extract routing keys from document write requests,
 * compare them to detect changes, and resolve shard locations.
 *
 * @opensearch.internal
 */
public class RoutingChangeDetector {

    /**
     * Data class to hold routing keys (index, id, and routing) that determine
     * which shard stores a document.
     *
     * @opensearch.internal
     */
    public static class RoutingKeys {
        private final String index;
        private final String id;
        private final String routing;

        /**
         * Constructs a new RoutingKeys instance.
         *
         * @param index the index name
         * @param id the document id
         * @param routing the routing value (may be null)
         */
        public RoutingKeys(String index, String id, String routing) {
            this.index = index;
            this.id = id;
            this.routing = routing;
        }

        /**
         * Gets the index name.
         *
         * @return the index name
         */
        public String getIndex() {
            return index;
        }

        /**
         * Gets the document id.
         *
         * @return the document id
         */
        public String getId() {
            return id;
        }

        /**
         * Gets the routing value.
         *
         * @return the routing value, or null if not set
         */
        public String getRouting() {
            return routing;
        }

        /**
         * Compares this RoutingKeys with another for equality.
         * Two RoutingKeys are equal if their index, id, and routing values are equal.
         *
         * @param other the other RoutingKeys to compare with
         * @return true if the routing keys are equal, false otherwise
         */
        public boolean equals(RoutingKeys other) {
            if (other == null) {
                return false;
            }
            return Objects.equals(index, other.index)
                && Objects.equals(id, other.id)
                && Objects.equals(routing, other.routing);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RoutingKeys that = (RoutingKeys) obj;
            return equals(that);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, id, routing);
        }

        @Override
        public String toString() {
            return "RoutingKeys{" +
                "index='" + index + '\'' +
                ", id='" + id + '\'' +
                ", routing='" + routing + '\'' +
                '}';
        }
    }

    /**
     * Extracts routing keys from a DocWriteRequest.
     *
     * @param request the document write request
     * @return the extracted routing keys
     */
    public static RoutingKeys extractRoutingKeys(DocWriteRequest<?> request) {
        return new RoutingKeys(
            request.index(),
            request.id(),
            request.routing()
        );
    }

    /**
     * Checks if routing keys have changed between two RoutingKeys instances.
     *
     * @param original the original routing keys
     * @param current the current routing keys
     * @return true if the routing keys have changed, false otherwise
     */
    public static boolean hasRoutingChanged(RoutingKeys original, RoutingKeys current) {
        return !original.equals(current);
    }

    /**
     * Resolves the shard ID for the given routing keys using the cluster state and operation routing.
     * This method reuses the existing OperationRouting.generateShardId() logic to determine which
     * shard should store a document based on its routing keys.
     *
     * @param keys the routing keys (index, id, routing) to resolve
     * @param clusterState the current cluster state
     * @return the resolved ShardId
     * @throws IndexNotFoundException if the index specified in the routing keys does not exist
     */
    public static ShardId resolveShardId(
        RoutingKeys keys,
        ClusterState clusterState
    ) {
        IndexMetadata indexMetadata = clusterState.metadata().index(keys.getIndex());
        if (indexMetadata == null) {
            throw new IndexNotFoundException(keys.getIndex());
        }

        Index index = indexMetadata.getIndex();

        int shardId = OperationRouting.generateShardId(indexMetadata, keys.getId(), keys.getRouting());

        return new ShardId(index, shardId);
    }
}
