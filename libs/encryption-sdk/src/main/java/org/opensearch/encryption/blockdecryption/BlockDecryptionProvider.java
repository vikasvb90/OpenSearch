/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.blockdecryption;

import java.io.InputStream;

/**
 * Used in partial decryption use cases.
 */
public interface BlockDecryptionProvider {

    /**
     * Provides decrypting stream for partial encrypted content
     * @param inputStream Encrypting stream of partial content to be decrypted.
     * @param startPosOfRawContent start position of partial raw content to be retrieved.
     * @param endPosOfRawContent end position of partial raw content to be retrieved.
     * @return Decrypting stream responsible for supplying decrypted content against encrypted stream.
     */
    InputStream provideBlockDecryptionStream(InputStream inputStream, long startPosOfRawContent, long endPosOfRawContent);

    /**
     * In some cases it might be required to estimate start and end position of the encrypted content against the range
     * of decrypted content.
     * @param decryptedRange Range in decrypted content.
     * @return Range in encrypted content corresponding to the range of decrypted content.
     */
    long[] getTransformedEncryptedRange(long[] decryptedRange);
}
