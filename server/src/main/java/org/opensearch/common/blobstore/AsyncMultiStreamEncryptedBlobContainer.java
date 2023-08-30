/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.EncryptionHandler;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;

/**
 * EncryptedBlobContainer is an encrypted BlobContainer that is backed by a
 * {@link AsyncMultiStreamBlobContainer}
 *
 * @opensearch.internal
 */
public class AsyncMultiStreamEncryptedBlobContainer<T extends EncryptionHandler, U> extends EncryptedBlobContainer<T, U> implements AsyncMultiStreamBlobContainer {

    private final AsyncMultiStreamBlobContainer blobContainer;
    private final CryptoHandler<T, U> cryptoHandler;

    public AsyncMultiStreamEncryptedBlobContainer(AsyncMultiStreamBlobContainer blobContainer, CryptoHandler<T, U>  cryptoHandler) {
        super(blobContainer, cryptoHandler);
        this.blobContainer = blobContainer;
        this.cryptoHandler  = cryptoHandler;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        EncryptedWriteContext<T, U> encryptedWriteContext = new EncryptedWriteContext<>(writeContext, cryptoHandler);
        blobContainer.asyncBlobUpload(encryptedWriteContext, completionListener);
    }

    @Override
    public boolean remoteIntegrityCheckSupported() {
        return false;
    }

    static class EncryptedWriteContext<T extends EncryptionHandler, U> extends WriteContext {

        private final Object encryptionMetadata;
        private final CryptoHandler<T, U>  cryptoHandler;
        private final long fileSize;

        /**
         * Construct a new encrypted WriteContext object
         */
        public EncryptedWriteContext(WriteContext writeContext, CryptoHandler<T, U>  cryptoHandler) {
            super(writeContext);
            this.cryptoHandler  = cryptoHandler;
            this.encryptionMetadata = this.cryptoHandler.initEncryptionMetadata();
            this.fileSize = this.cryptoHandler.estimateEncryptedLengthOfEntireContent((T) encryptionMetadata, writeContext.getFileSize());
        }

        public StreamContext getStreamProvider(long partSize) {
            long adjustedPartSize = cryptoHandler.adjustContentSizeForPartialEncryption((T) encryptionMetadata, partSize);
            StreamContext streamContext = super.getStreamProvider(adjustedPartSize);
            return new EncryptedStreamContext<>(streamContext, cryptoHandler, encryptionMetadata);
        }

        /**
         * @return The total size of the encrypted file
         */
        public long getFileSize() {
            return fileSize;
        }
    }

    static class EncryptedStreamContext<T extends EncryptionHandler, U> extends StreamContext {

        private final CryptoHandler<T, U>  cryptoHandler;
        private final Object encryptionMetadata;

        /**
         * Construct a new encrypted StreamContext object
         */
        public EncryptedStreamContext(StreamContext streamContext, CryptoHandler<T, U>  cryptoHandler, Object encryptionMetadata) {
            super(streamContext);
            this.cryptoHandler = cryptoHandler;
            this.encryptionMetadata = encryptionMetadata;
        }

        @Override
        public InputStreamContainer provideStream(int partNumber) throws IOException {
            InputStreamContainer inputStreamContainer = super.provideStream(partNumber);
            return cryptoHandler.createEncryptingStreamOfPart((T) encryptionMetadata, inputStreamContainer, getNumberOfParts(), partNumber);
        }

    }
}
