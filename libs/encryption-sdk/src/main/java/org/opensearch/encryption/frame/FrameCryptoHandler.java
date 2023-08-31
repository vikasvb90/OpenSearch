/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption.frame;

import com.amazonaws.encryptionsdk.ParsedCiphertext;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.encryption.frame.core.AwsCrypto;
import org.opensearch.encryption.frame.core.EncryptionMetadata;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class FrameCryptoHandler implements CryptoHandler<EncryptionMetadata, ParsedCiphertext> {
    private final AwsCrypto awsCrypto;
    private final Map<String, String> encryptionContext;

    // package private for tests
    private final int FRAME_SIZE = 8 * 1024;

    public FrameCryptoHandler(AwsCrypto awsCrypto, Map<String, String> encryptionContext) {
        this.awsCrypto = awsCrypto;
        this.encryptionContext = encryptionContext;
    }

    public int getFrameSize() {
        return FRAME_SIZE;
    }

    /**
     * Initialises metadata store used in encryption.
     * @return crypto metadata object constructed with encryption metadata like data key pair, encryption algorithm, etc.
     */
    public EncryptionMetadata initEncryptionMetadata() {
        return awsCrypto.createCryptoContext(encryptionContext, getFrameSize());
    }

    /**
     * Context: This SDK uses Frame encryption which means that encrypted content is composed of frames i.e., a frame
     * is the smallest unit of encryption or decryption.
     * Due to this in cases where more than one stream is used to produce content, each stream content except the
     * last should line up along the frame boundary i.e. there can't be any partial frame.
     * Hence, size of each stream except the last, should be exactly divisible by the frame size and therefore, this
     * method should be called before committing on the stream size.
     * This is not required if number of streams for a content is only 1.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param streamSize Size of the stream to be adjusted.
     * @return Adjusted size of the stream.
     */
    public long adjustContentSizeForPartialEncryption(EncryptionMetadata cryptoContextObj, long streamSize) {
        EncryptionMetadata encryptionMetadata = validateEncryptionMetadata(cryptoContextObj);
        return (streamSize - (streamSize % encryptionMetadata.getFrameSize())) + encryptionMetadata.getFrameSize();
    }

    /**
     * Estimate length of the encrypted stream.
     *
     * @param cryptoMetadataObj crypto metadata instance
     * @param contentLength Size of the raw content
     * @return Calculated size of the encrypted stream for the provided raw stream.
     */
    public long estimateEncryptedLengthOfEntireContent(EncryptionMetadata cryptoMetadataObj, long contentLength) {
        EncryptionMetadata encryptionMetadata = validateEncryptionMetadata(cryptoMetadataObj);
        return encryptionMetadata.getCiphertextHeaderBytes().length + awsCrypto.estimateOutputSizeWithFooter(
            encryptionMetadata.getFrameSize(),
            encryptionMetadata.getNonceLen(),
            encryptionMetadata.getCryptoAlgo().getTagLen(),
            contentLength,
            encryptionMetadata.getCryptoAlgo()
        );
    }

    /**
     * Estimate length of the decrypted stream.
     *
     * @param cryptoMetadataObj crypto metadata instance
     * @param contentLength Size of the encrypted content
     * @return Calculated size of the encrypted stream for the provided raw stream.
     */
    public long estimateDecryptedLength(ParsedCiphertext cryptoMetadataObj, long contentLength) {
        return awsCrypto.estimateDecryptedSize(
            cryptoMetadataObj.getFrameLength(),
            cryptoMetadataObj.getNonceLength(),
            cryptoMetadataObj.getCryptoAlgoId().getTagLen(),
            contentLength - cryptoMetadataObj.getOffset(),
            cryptoMetadataObj.getCryptoAlgoId()
        );
    }

    /**
     * Wraps a raw InputStream with encrypting stream
     * @param cryptoContextObj consists encryption metadata.
     * @param stream Raw InputStream to encrypt
     * @return encrypting stream wrapped around raw InputStream.
     */
    public InputStreamContainer createEncryptingStream(EncryptionMetadata cryptoContextObj, InputStreamContainer stream) {
        EncryptionMetadata encryptionMetadata = validateEncryptionMetadata(cryptoContextObj);
        return createEncryptingStreamOfPart(encryptionMetadata, stream, 1, 0);
    }

    private EncryptionMetadata validateEncryptionMetadata(EncryptionMetadata cryptoContext) {
        return cryptoContext;
    }

    /**
     * Provides encrypted stream for a raw stream emitted for a part of content. This method doesn't require streams of
     * the content to be provided in sequence and is thread safe.
     * Note: This method assumes that all streams except the last stream are of same size. Also, length of the stream
     * except the last index must exactly align with frame length.
     *
     * @param cryptoContextObj stateful object for a request consisting of materials required in encryption.
     * @param stream raw stream for which encrypted stream has to be created.
     * @param totalStreams Number of streams being used for the entire content.
     * @param streamIdx Index of the current stream.
     * @return Encrypted stream for the provided raw stream.
     */
    public InputStreamContainer createEncryptingStreamOfPart(
        EncryptionMetadata cryptoContextObj,
        InputStreamContainer stream,
        int totalStreams,
        int streamIdx
    ) {
        EncryptionMetadata encryptionMetadata = parseEncryptionMetadata(cryptoContextObj);

        int frameStartNumber = (int) (stream.getOffset() / getFrameSize()) + 1;

        return awsCrypto.createEncryptingStream(stream, streamIdx, totalStreams, frameStartNumber, encryptionMetadata);
    }

    private EncryptionMetadata parseEncryptionMetadata(Object cryptoContextObj) {
        if (!(cryptoContextObj instanceof EncryptionMetadata)) {
            throw new IllegalArgumentException("Unknown crypto metadata object received");
        }
        return (EncryptionMetadata) cryptoContextObj;
    }

    /**
     *
     * @param encryptedHeaderContentSupplier Supplier used to fetch bytes from source for header creation
     * @return parsed encryption metadata object
     * @throws IOException if content fetch for header creation fails
     */
    public ParsedCiphertext loadEncryptionMetadata(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) throws IOException {
        byte[] encryptedHeader = encryptedHeaderContentSupplier.supply(0, 4095);
        return new ParsedCiphertext(encryptedHeader);
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
     * Provides trailing signature length if any based on the crypto algorithm used.
     * @param cryptoContextObj Context object needed to calculate trailing length.
     * @return Trailing signature length
     */
    public int getTrailingSignatureLength(Object cryptoContextObj) {
        EncryptionMetadata encryptionMetadata = parseEncryptionMetadata(cryptoContextObj);
        return awsCrypto.getTrailingSignatureSize(encryptionMetadata.getCryptoAlgo());
    }

    private InputStream createBlockDecryptionStream(
        Object cryptoContext,
        InputStream inputStream,
        long startPosOfRawContent,
        long endPosOfRawContent,
        long[] encryptedRange
    ) {
        ParsedCiphertext parsedCiphertext = (ParsedCiphertext) cryptoContext;
        if (startPosOfRawContent % parsedCiphertext.getFrameLength() != 0
            || (endPosOfRawContent + 1) % parsedCiphertext.getFrameLength() != 0) {
            throw new IllegalArgumentException("Start and end positions of the raw content must be aligned with frame length");
        }
        int frameStartNumber = (int) (startPosOfRawContent / parsedCiphertext.getFrameLength()) + 1;
        long encryptedSize = encryptedRange[1] - encryptedRange[0] + 1;
        return awsCrypto.createDecryptingStream(inputStream, encryptedSize, parsedCiphertext, frameStartNumber, false);
    }

    /**
     * For partial reads of encrypted content, frame based encryption requires the range of content to be adjusted for
     * successful decryption. Adjusted range may or may not be same as the provided range. If range is adjusted then
     * starting offset of resultant range can be lesser than the starting offset of provided range and end
     * offset can be greater than the ending offset of the provided range.
     * It provides supplier for creating decrypted stream out of the provided encrypted stream. Decrypted content is
     * trimmed down to the desired range with the help of bounded stream. This method assumes that provided encrypted
     * stream supplies content for the adjusted range.
     *
     * @param cryptoContext crypto metadata instance consisting of encryption metadata used in encryption.
     * @param startPosOfRawContent starting position in the raw/decrypted content
     * @param endPosOfRawContent ending position in the raw/decrypted content
     * @return stream provider for decrypted stream for the specified range of content including adjusted range
     */
    public DecryptedRangedStreamProvider createDecryptingStreamOfRange(
        ParsedCiphertext cryptoContext,
        long startPosOfRawContent,
        long endPosOfRawContent
    ) {
        if (!(cryptoContext instanceof ParsedCiphertext)) {
            throw new IllegalArgumentException("Unknown crypto metadata object received for adjusting range for decryption");
        }
        long adjustedStartPos = startPosOfRawContent - (startPosOfRawContent % cryptoContext.getFrameLength());
        long endPosOverhead = (endPosOfRawContent + 1) % cryptoContext.getFrameLength();
        long adjustedEndPos = endPosOverhead == 0
            ? endPosOfRawContent
            : (endPosOfRawContent - endPosOverhead + cryptoContext.getFrameLength());
        long[] encryptedRange = transformToEncryptedRange(cryptoContext, adjustedStartPos, adjustedEndPos);
        return new DecryptedRangedStreamProvider(encryptedRange, (encryptedStream) -> {
            InputStream decryptedStream = createBlockDecryptionStream(
                cryptoContext,
                encryptedStream,
                adjustedStartPos,
                adjustedEndPos,
                encryptedRange
            );
            return new TrimmingStream(adjustedStartPos, adjustedEndPos, startPosOfRawContent, endPosOfRawContent, decryptedStream);
        });
    }

    private long[] transformToEncryptedRange(ParsedCiphertext parsedCiphertext, long startPosOfRawContent, long endPosOfRawContent) {

        long startPos = awsCrypto.estimatePartialOutputSize(
            parsedCiphertext.getFrameLength(),
            parsedCiphertext.getCryptoAlgoId().getNonceLen(),
            parsedCiphertext.getCryptoAlgoId().getTagLen(),
            startPosOfRawContent
        ) + parsedCiphertext.getOffset();

        long endPos = awsCrypto.estimatePartialOutputSize(
            parsedCiphertext.getFrameLength(),
            parsedCiphertext.getCryptoAlgoId().getNonceLen(),
            parsedCiphertext.getCryptoAlgoId().getTagLen(),
            endPosOfRawContent
        ) + parsedCiphertext.getOffset();

        return new long[] { startPos, endPos };
    }

}
