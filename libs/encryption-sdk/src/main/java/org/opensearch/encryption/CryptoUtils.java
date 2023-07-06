/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import com.amazonaws.encryptionsdk.ParsedCiphertext;
import org.opensearch.encryption.blockdecryption.BlockDecryptionProvider;
import org.opensearch.encryption.blockdecryption.EncryptedHeaderContentSupplier;
import org.opensearch.encryption.core.AwsCrypto;
import org.opensearch.encryption.core.CryptoContext;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class CryptoUtils {
    private final AwsCrypto awsCrypto;
    private final Map<String, String> encryptionContext;

    // package private for tests
    static int FRAME_SIZE = 8 * 1024;

    public CryptoUtils(AwsCrypto awsCrypto, Map<String, String> encryptionContext) {
        this.awsCrypto = awsCrypto;
        this.encryptionContext = encryptionContext;
    }

    public int getFrameSize() {
        return FRAME_SIZE;
    }

    /**
     * Initialises a stateful crypto context used in encryption.
     * @return crypto context object constructed with encryption metadata like data key pair, encryption algorithm, etc.
     */
    public Object initCryptoContext() {
        return awsCrypto.createCryptoContext(encryptionContext);
    }

    /**
     * Context: This SDK uses Frame encryption which means that encrypted content is composed of frames i.e., a frame
     * is the smallest unit of encryption or decryption.
     * Due to this for more than 1 stream emitted for a content, each stream content except the last should line up
     * along the frame boundary i.e. there can't be any partial frame.
     * Hence, size of each stream except the last, should be exactly divisible by the frame size and therefore, this
     * method should be called before committing on the stream size.
     * This is not required if number of streams for a content is only 1.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param streamSize Size of the stream to be adjusted.
     * @return Adjusted size of the stream.
     */
    public long adjustEncryptedStreamSize(Object cryptoContextObj, long streamSize) {
        CryptoContext cryptoContext = validateCryptoContext(cryptoContextObj);
        return (streamSize - (streamSize % cryptoContext.getFrameSize())) + cryptoContext.getFrameSize();
    }

    /**
     * Estimate length of the encrypted stream.
     *
     * @param cryptoContextObj crypto context instance
     * @param contentLength Size of the raw content
     * @return Calculated size of the encrypted stream for the provided raw stream.
     */
    public long estimateEncryptedLength(Object cryptoContextObj, long contentLength) {
        CryptoContext cryptoContext = validateCryptoContext(cryptoContextObj);
        return cryptoContext.getCiphertextHeaderBytes().length + awsCrypto.estimateOutputSizeWithFooter(
            cryptoContext.getFrameSize(),
            cryptoContext.getNonceLen(),
            cryptoContext.getCryptoAlgo().getTagLen(),
            contentLength,
            cryptoContext.getCryptoAlgo()
        );
    }

    /**
     * Wraps a raw InputStream with encrypting stream
     * @param cryptoContextObj consists encryption metadata.
     * @param stream Raw InputStream to encrypt
     * @return encrypting stream wrapped around raw InputStream.
     */
    public InputStreamContainer createEncryptingStream(Object cryptoContextObj, InputStreamContainer stream) {
        CryptoContext cryptoContext = validateCryptoContext(cryptoContextObj);
        return createEncryptingStreamOfPart(cryptoContext, stream, 1, 0);
    }

    private CryptoContext validateCryptoContext(Object cryptoContext) {
        if (!(cryptoContext instanceof CryptoContext)) {
            throw new IllegalArgumentException("Unknown crypto context object received");
        }
        return (CryptoContext) cryptoContext;
    }

    /**
     * Provides encrypted stream for a raw stream emitted for a part of content. This method doesn't require streams of
     * the content to be provided in sequence and is thread safe.
     * Note: This method assumes that all streams except the last stream are of same size.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param stream raw stream for which encrypted stream has to be created.
     * @param totalStreams Number of streams being used for the entire content.
     * @param streamIdx Index of the current stream.
     * @return Encrypted stream for the provided raw stream.
     */
    public InputStreamContainer createEncryptingStreamOfPart(
        Object cryptoContextObj,
        InputStreamContainer stream,
        int totalStreams,
        int streamIdx
    ) {
        CryptoContext cryptoContext = parseCryptoContext(cryptoContextObj);

        boolean includeHeader = streamIdx == 0;
        boolean includeFooter = streamIdx == (totalStreams - 1);
        int frameStartNumber = (int) (stream.getOffset() / getFrameSize()) + 1;

        return awsCrypto.createEncryptingStream(
            stream,
            streamIdx,
            totalStreams,
            frameStartNumber,
            includeHeader,
            includeFooter,
            cryptoContext
        );
    }

    private CryptoContext parseCryptoContext(Object cryptoContextObj) {
        if (!(cryptoContextObj instanceof CryptoContext)) {
            throw new IllegalArgumentException("Unknown crypto context object received");
        }
        return (CryptoContext) cryptoContextObj;
    }

    /**
     * This method accepts an encrypted stream and provides a decrypting wrapper.
     *
     * @param encryptedStream to be decrypted.
     * @return Decrypting wrapper stream
     */
    public InputStream createDecryptingStream(InputStream encryptedStream) {
        return awsCrypto.createDecryptingStream(encryptedStream);
    }

    /**
     * Provides trailing sigature length if any based on the crypto algorithm used.
     * @param cryptoContextObj Context object needed to calculate trailing length.
     * @return Trailing signature length
     */
    public int getTrailingSignatureLength(Object cryptoContextObj) {
        CryptoContext cryptoContext = parseCryptoContext(cryptoContextObj);
        return awsCrypto.getTrailingSignatureSize(cryptoContext.getCryptoAlgo());
    }

    /**
     * This method creates a {@link BlockDecryptionProvider} which provides a wrapped stream to decrypt the
     * underlying stream. This is a file level cachable version of
     * {@link #createBlockDecryptionProvider(EncryptedHeaderContentSupplier)} where {@link BlockDecryptionProvider}
     * can be created and cached once for first block decryption of a file and then it can be reused for all subsequent
     * decryption of any block of the same file. This helps in avoiding parsing file header in other decryption
     * requests of blocks of the same file.
     *
     * @param encryptedHeaderContentSupplier Supplier used to fetch bytes from source for header creation
     * @return Block decryption supplier
     * @throws IOException if creation of block decryption supplier fails
     */
    public BlockDecryptionProvider createBlockDecryptionProvider(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier)
        throws IOException {
        byte[] encryptedHeader = encryptedHeaderContentSupplier.supply(0, 4095);
        ParsedCiphertext parsedCiphertext = new ParsedCiphertext(encryptedHeader);

        return new BlockDecryptionProvider() {
            @Override
            public InputStream provideBlockDecryptionStream(InputStream inputStream, long startPosOfRawContent, long endPosOfRawContent) {
                int frameStartNumber = (int) (startPosOfRawContent / getFrameSize()) + 1;
                long[] encryptedRange = getTransformedEncryptedRange(new long[] { startPosOfRawContent, endPosOfRawContent });
                long encryptedSize = encryptedRange[1] - encryptedRange[0] + 1;
                return awsCrypto.createDecryptingStream(inputStream, encryptedSize, parsedCiphertext, frameStartNumber, false);
            }

            @Override
            public long[] getTransformedEncryptedRange(long[] decryptedRange) {
                return transformToEncryptedRange(parsedCiphertext, decryptedRange);
            }
        };

    }

    private long[] transformToEncryptedRange(ParsedCiphertext parsedCiphertext, long[] range) {

        long startPos = awsCrypto.estimatePartialOutputSize(
            parsedCiphertext.getFrameLength(),
            parsedCiphertext.getCryptoAlgoId().getNonceLen(),
            parsedCiphertext.getCryptoAlgoId().getTagLen(),
            range[0]
        ) + parsedCiphertext.getOffset();
        if (range.length == 1) {
            return new long[] { startPos };
        }
        long endPos = awsCrypto.estimatePartialOutputSize(
            parsedCiphertext.getFrameLength(),
            parsedCiphertext.getCryptoAlgoId().getNonceLen(),
            parsedCiphertext.getCryptoAlgoId().getTagLen(),
            range[1]
        ) + parsedCiphertext.getOffset();

        return new long[] { startPos, endPos };
    }
}
