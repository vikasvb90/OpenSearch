/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.Set;

/**
 * An extension of {@link BlobContainer} that adds {@link AsyncMultiStreamBlobContainer#asyncBlobUpload} to allow
 * multipart uploads and performs integrity checks on transferred files
 *
 * @opensearch.internal
 */
public interface AsyncMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext         A WriteContext object encapsulating all information needed to perform the upload
     * @param completionListener   Listener on which upload events should be published.
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException;

    /**
     * Creates an async callback of a {@link ReadContext} containing the multipart streams for a specified blob within the container.
     * @param blobName The name of the blob for which the {@link ReadContext} needs to be fetched.
     * @param listener  Async listener for {@link ReadContext} object which serves the input streams and other metadata for the blob
     */
    @ExperimentalApi
    void readBlobAsync(String blobName, ActionListener<ReadContext> listener);

    /*
     * Wether underlying blobContainer can verify integrity of data after transfer. If true and if expected
     * checksum is provided in WriteContext, then the checksum of transferred data is compared with expected checksum
     * by underlying blobContainer. In this case, caller doesn't need to ensure integrity of data.
     */
    boolean remoteIntegrityCheckSupported();

    /**
     * Should copy files from source remote directory to current remote directory if any of the provided files
     * doesn't already exist.
     * It should first make sure that all provided files are present in source directory. Also, it should make an exact
     * copy of the source file. This means that all parts if applicable should be an exact replica and metadata and tags
     * should be copied over.
     *
     * @param files Files to be copied from source.
     * @param blobContainer Source container from which files are to be copied.
     * @return Subset of files from the provided files which were actually copied from source to target depending on the
     *         availability of file in source remote directory/
     */
    Set<String> copyFilesFromSrcRemote(Set<String> files, AsyncMultiStreamBlobContainer blobContainer);
}
