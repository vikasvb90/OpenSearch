/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.core;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.CryptoHandler;
import com.amazonaws.encryptionsdk.internal.MessageCryptoHandler;
import com.amazonaws.encryptionsdk.internal.ProcessingSummary;
import com.amazonaws.encryptionsdk.model.CiphertextFooters;
import com.amazonaws.encryptionsdk.model.CiphertextHeaders;
import com.amazonaws.encryptionsdk.model.CiphertextType;
import com.amazonaws.encryptionsdk.model.KeyBlob;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DERSequence;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.ECPrivateKey;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class implements the CryptoHandler interface by providing methods for the encryption of
 * plaintext data.
 *
 * <p>This class creates the ciphertext headers and delegates the encryption of the plaintext to the
 * {@link FrameEncryptionHandler}.
 */
public class EncryptionHandler implements MessageCryptoHandler {

    private final Map<String, String> encryptionContext_;
    private final CryptoAlgorithm cryptoAlgo_;
    private final List<MasterKey> masterKeys_;
    private final List<KeyBlob> keyBlobs_;
    private final SecretKey encryptionKey_;
    private final byte version_;
    private final CiphertextType type_;
    private final byte nonceLen_;

    private final CiphertextHeaders ciphertextHeaders_;
    private final byte[] ciphertextHeaderBytes_;
    private final CryptoHandler contentCryptoHandler_;

    private boolean firstOperation_;
    private boolean complete_ = false;

    private long plaintextBytes_ = 0;
    private long plaintextByteLimit_ = -1;

    private final PrivateKey trailingSignaturePrivateKey;
    private final MessageDigest trailingDigest;
    private final Signature trailingSig;

    /**
     * Create an encryption handler using the provided master key and encryption context.
     * @param cryptoContext Context object created before encryption
     * @param isFirstStream In case of first stream, file header is additionally created which consists of crypto
     *                      materials.
     * @param frameStartNumber Number from which assignment has to start for new frames
     */
    public EncryptionHandler(CryptoContext cryptoContext, boolean isFirstStream, int frameStartNumber) throws AwsCryptoException {
        encryptionContext_ = cryptoContext.getEncryptionContext();
        cryptoAlgo_ = cryptoContext.getCryptoAlgo();
        masterKeys_ = cryptoContext.getMasterKeys();
        keyBlobs_ = cryptoContext.getKeyBlobs();
        encryptionKey_ = cryptoContext.getEncryptionKey();
        version_ = cryptoContext.getVersion();
        type_ = cryptoContext.getType();
        nonceLen_ = cryptoContext.getNonceLen();
        ciphertextHeaders_ = cryptoContext.getCiphertextHeaders();
        ciphertextHeaderBytes_ = cryptoContext.getCiphertextHeaderBytes();
        firstOperation_ = isFirstStream;
        byte[] messageId = cryptoContext.getMessageId();
        trailingSignaturePrivateKey = cryptoContext.getTrailingSignaturePrivateKey();
        trailingDigest = cryptoContext.getTrailingDigest();
        trailingSig = cryptoContext.getTrailingSig();
        contentCryptoHandler_ = new FrameEncryptionHandler(
            encryptionKey_,
            nonceLen_,
            cryptoAlgo_,
            messageId,
            cryptoContext.getFrameSize(),
            frameStartNumber
        );
    }

    /**
     * Encrypt a block of bytes from {@code in} putting the plaintext result into {@code out}.
     *
     * <p>It encrypts by performing the following operations:
     *
     * <ol>
     *   <li>if this is the first call to encrypt, write the ciphertext headers to the output being
     *       returned.
     *   <li>else, pass off the input data to underlying content cryptohandler.
     * </ol>
     *
     * @param in the input byte array.
     * @param off the offset into the in array where the data to be encrypted starts.
     * @param len the number of bytes to be encrypted.
     * @param out the output buffer the encrypted bytes go into.
     * @param outOff the offset into the output byte array the encrypted data starts at.
     * @return the number of bytes written to out and processed
     * @throws AwsCryptoException if len or offset values are negative.
     * @throws BadCiphertextException thrown by the underlying cipher handler.
     */
    @Override
    public ProcessingSummary processBytes(final byte[] in, final int off, final int len, final byte[] out, final int outOff)
        throws AwsCryptoException, BadCiphertextException {
        if (len < 0 || off < 0) {
            throw new AwsCryptoException(
                String.format(Locale.getDefault(), "Invalid values for input offset: %d and length: %d", off, len)
            );
        }

        checkPlaintextSizeLimit(len);

        int actualOutLen = 0;

        if (firstOperation_) {
            System.arraycopy(ciphertextHeaderBytes_, 0, out, outOff, ciphertextHeaderBytes_.length);
            actualOutLen += ciphertextHeaderBytes_.length;

            firstOperation_ = false;
        }

        ProcessingSummary contentOut = contentCryptoHandler_.processBytes(in, off, len, out, outOff + actualOutLen);
        actualOutLen += contentOut.getBytesWritten();
        updateTrailingSignature(out, outOff, actualOutLen);
        plaintextBytes_ += contentOut.getBytesProcessed();
        return new ProcessingSummary(actualOutLen, contentOut.getBytesProcessed());
    }

    /**
     * Finish encryption of the plaintext bytes.
     *
     * @param out space for any resulting output data.
     * @param outOff offset into out to start copying the data at.
     * @return number of bytes written into out.
     * @throws BadCiphertextException thrown by the underlying cipher handler.
     */
    @Override
    public int doFinal(final byte[] out, final int outOff) throws BadCiphertextException {
        if (complete_) {
            throw new IllegalStateException("Attempted to call doFinal twice");
        }

        complete_ = true;

        checkPlaintextSizeLimit(0);

        int written = contentCryptoHandler_.doFinal(out, outOff);
        updateTrailingSignature(out, outOff, written);
        if (cryptoAlgo_.getTrailingSignatureLength() > 0) {
            try {
                CiphertextFooters footer = new CiphertextFooters(signContent());
                byte[] fBytes = footer.toByteArray();
                System.arraycopy(fBytes, 0, out, outOff + written, fBytes.length);
                return written + fBytes.length;
            } catch (final SignatureException ex) {
                throw new AwsCryptoException(ex);
            }
        } else {
            return written;
        }
    }

    private byte[] signContent() throws SignatureException {
        if (trailingDigest != null) {
            if (!trailingSig.getAlgorithm().contains("ECDSA")) {
                throw new UnsupportedOperationException("Signatures calculated in pieces is only supported for ECDSA.");
            }
            final byte[] digest = trailingDigest.digest();
            return generateEcdsaFixedLengthSignature(digest);
        }
        return trailingSig.sign();
    }

    private byte[] generateEcdsaFixedLengthSignature(final byte[] digest) throws SignatureException {
        byte[] signature;
        // Unfortunately, we need deterministic lengths some signatures are non-deterministic in length.
        // So, retry until we get the right length :-(
        do {
            trailingSig.update(digest);
            signature = trailingSig.sign();
            if (signature.length != cryptoAlgo_.getTrailingSignatureLength()) {
                // Most of the time, a signature of the wrong length can be fixed
                // be negating s in the signature relative to the group order.
                ASN1Sequence seq = ASN1Sequence.getInstance(signature);
                ASN1Integer r = (ASN1Integer) seq.getObjectAt(0);
                ASN1Integer s = (ASN1Integer) seq.getObjectAt(1);
                ECPrivateKey ecKey = (ECPrivateKey) trailingSignaturePrivateKey;
                s = new ASN1Integer(ecKey.getParams().getOrder().subtract(s.getPositiveValue()));
                seq = new DERSequence(new ASN1Encodable[] { r, s });
                try {
                    signature = seq.getEncoded();
                } catch (IOException ex) {
                    throw new SignatureException(ex);
                }
            }
        } while (signature.length != cryptoAlgo_.getTrailingSignatureLength());
        return signature;
    }

    /**
     * Return the size of the output buffer required for a {@code processBytes} plus a {@code doFinal}
     * with an input of inLen bytes.
     *
     * @param inLen the length of the input.
     * @return the space required to accommodate a call to processBytes and doFinal with len bytes of
     *     input.
     */
    @Override
    public int estimateOutputSize(final int inLen) {
        int outSize = 0;
        if (firstOperation_ == true) {
            outSize += ciphertextHeaderBytes_.length;
        }
        outSize += contentCryptoHandler_.estimateOutputSize(inLen);

        outSize += getAlgoTrailingLength(cryptoAlgo_);

        return outSize;
    }

    public static int getAlgoTrailingLength(CryptoAlgorithm cryptoAlgo) {
        int outSize = 0;
        if (cryptoAlgo.getTrailingSignatureLength() > 0) {
            outSize += 2; // Length field in footer
            outSize += cryptoAlgo.getTrailingSignatureLength();
        }

        return outSize;
    }

    @Override
    public int estimatePartialOutputSize(int inLen) {
        int outSize = 0;
        if (firstOperation_ == true) {
            outSize += ciphertextHeaderBytes_.length;
        }
        outSize += contentCryptoHandler_.estimatePartialOutputSize(inLen);

        return outSize;
    }

    @Override
    public int estimateFinalOutputSize() {
        return estimateOutputSize(0);
    }

    /**
     * Return the encryption context.
     *
     * @return the key-value map containing encryption context.
     */
    @Override
    public Map<String, String> getEncryptionContext() {
        return encryptionContext_;
    }

    @Override
    public CiphertextHeaders getHeaders() {
        return ciphertextHeaders_;
    }

    @Override
    public void setMaxInputLength(long size) {
        if (size < 0) {
            throw Utils.cannotBeNegative("Max input length");
        }

        if (plaintextByteLimit_ == -1 || plaintextByteLimit_ > size) {
            plaintextByteLimit_ = size;
        }

        // check that we haven't already exceeded the limit
        checkPlaintextSizeLimit(0);
    }

    private void checkPlaintextSizeLimit(long additionalBytes) {
        if (plaintextByteLimit_ != -1 && plaintextBytes_ + additionalBytes > plaintextByteLimit_) {
            throw new IllegalStateException("Plaintext size exceeds max input size limit");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<? extends MasterKey<?>> getMasterKeys() {
        // noinspection unchecked
        return (List) masterKeys_; // This is unmodifiable
    }

    private void updateTrailingSignature(byte[] input, int offset, int len) {
        if (this.trailingDigest != null) {
            this.trailingDigest.update(input, offset, len);
        } else if (this.trailingSig != null) {
            try {
                this.trailingSig.update(input, offset, len);
            } catch (SignatureException var5) {
                throw new AwsCryptoException(var5);
            }
        }

    }

    @Override
    public boolean isComplete() {
        return complete_;
    }
}
