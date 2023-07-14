/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.core;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.ParsedCiphertext;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.internal.LazyMessageCryptoHandler;
import com.amazonaws.encryptionsdk.internal.MessageCryptoHandler;
import com.amazonaws.encryptionsdk.internal.SignaturePolicy;
import com.amazonaws.encryptionsdk.model.EncryptionMaterialsRequest;
import org.opensearch.common.io.InputStreamContainer;

import java.io.InputStream;
import java.util.Map;

public class AwsCrypto {
    private final CryptoMaterialsManager materialsManager;
    private static final CommitmentPolicy DEFAULT_COMMITMENT_POLICY = CommitmentPolicy.RequireEncryptRequireDecrypt;
    private final int frameSize;
    private final CryptoAlgorithm cryptoAlgorithm;

    public AwsCrypto(final CryptoMaterialsManager materialsManager, final int frameSize, final CryptoAlgorithm cryptoAlgorithm) {
        Utils.assertNonNull(materialsManager, "materialsManager");
        if (frameSize <= 0) {
            throw new IllegalArgumentException("frameSize must be non-negative");
        }
        this.materialsManager = materialsManager;
        this.cryptoAlgorithm = cryptoAlgorithm;
        this.frameSize = frameSize;

    }

    public CryptoContext createCryptoContext(final Map<String, String> encryptionContext) {
        Utils.assertNonNull(encryptionContext, "encryptionContext");
        EncryptionMaterialsRequest.Builder requestBuilder = EncryptionMaterialsRequest.newBuilder()
            .setContext(encryptionContext)
            .setRequestedAlgorithm(cryptoAlgorithm)
            .setPlaintextSize(0) // To avoid skipping cache
            .setCommitmentPolicy(DEFAULT_COMMITMENT_POLICY);

        return new CryptoContext(frameSize, materialsManager.getMaterialsForEncrypt(requestBuilder.build()), DEFAULT_COMMITMENT_POLICY);
    }

    public InputStreamContainer createEncryptingStream(
        final InputStreamContainer stream,
        int streamIdx,
        int totalStreams,
        int frameNumber,
        boolean includeHeader,
        boolean includeFooter,
        CryptoContext cryptoContext
    ) {

        boolean isLastStream = streamIdx == totalStreams - 1 && includeFooter;
        boolean firstOperation = streamIdx == 0 && includeHeader;
        if (stream.getContentLength() % cryptoContext.getFrameSize() != 0 && !isLastStream) {
            throw new AwsCryptoException(
                "Length of each inputStream should be exactly divisible by frame size except "
                    + "the last inputStream. Current frame size is "
                    + cryptoContext.getFrameSize()
                    + " and inputStream length is "
                    + stream.getContentLength()
            );
        }
        final MessageCryptoHandler cryptoHandler = getEncryptingStreamHandler(frameNumber, firstOperation, cryptoContext);
        CryptoInputStream<?> cryptoInputStream = new CryptoInputStream<>(stream.getInputStream(), cryptoHandler, isLastStream);
        cryptoInputStream.setMaxInputLength(stream.getContentLength());

        long encryptedLength = 0;
        if (streamIdx == 0) {
            encryptedLength = cryptoContext.getCiphertextHeaderBytes().length;
        }
        if (streamIdx == (totalStreams - 1)) {
            encryptedLength += estimateOutputSizeWithFooter(
                cryptoContext.getFrameSize(),
                cryptoContext.getNonceLen(),
                cryptoContext.getCryptoAlgo().getTagLen(),
                stream.getContentLength(),
                cryptoContext.getCryptoAlgo()
            );
        } else {
            encryptedLength += estimatePartialOutputSize(
                cryptoContext.getFrameSize(),
                cryptoContext.getNonceLen(),
                cryptoContext.getCryptoAlgo().getTagLen(),
                stream.getContentLength()
            );
        }
        return new InputStreamContainer(cryptoInputStream, encryptedLength, -1);
    }

    public MessageCryptoHandler getEncryptingStreamHandler(int frameStartNumber, boolean firstOperation, CryptoContext cryptoContext) {
        return new LazyMessageCryptoHandler(info -> new EncryptionHandler(cryptoContext, firstOperation, frameStartNumber));
    }

    public long estimatePartialOutputSize(int frameLen, int nonceLen, int tagLen, long contentLength) {
        return FrameEncryptionHandler.estimatePartialSizeFromMetadata(contentLength, false, frameLen, nonceLen, tagLen);
    }

    public long estimateOutputSizeWithFooter(int frameLen, int nonceLen, int tagLen, long contentLength, CryptoAlgorithm cryptoAlgorithm) {
        return FrameEncryptionHandler.estimatePartialSizeFromMetadata(contentLength, true, frameLen, nonceLen, tagLen)
            + getTrailingSignatureSize(cryptoAlgorithm);
    }

    public int getTrailingSignatureSize(CryptoAlgorithm cryptoAlgorithm) {
        return EncryptionHandler.getAlgoTrailingLength(cryptoAlgorithm);
    }

    public CryptoInputStream<?> createDecryptingStream(final InputStream inputStream) {

        final MessageCryptoHandler cryptoHandler = DecryptionHandler.create(
            materialsManager,
            DEFAULT_COMMITMENT_POLICY,
            SignaturePolicy.AllowEncryptAllowDecrypt,
            1
        );
        CryptoInputStream<?> cryptoInputStream = new CryptoInputStream<>(inputStream, cryptoHandler, true, null);
        return cryptoInputStream;
    }

    public CryptoInputStream<?> createDecryptingStream(
        final InputStream inputStream,
        final long size,
        final ParsedCiphertext parsedCiphertext,
        final int frameStartNum,
        boolean lastPart
    ) {

        final MessageCryptoHandler cryptoHandler = DecryptionHandler.create(
            materialsManager,
            parsedCiphertext,
            DEFAULT_COMMITMENT_POLICY,
            SignaturePolicy.AllowEncryptAllowDecrypt,
            1,
            frameStartNum
        );
        CryptoInputStream<?> cryptoInputStream = new CryptoInputStream<>(inputStream, cryptoHandler, lastPart);
        cryptoInputStream.setMaxInputLength(size);
        return cryptoInputStream;
    }

}
