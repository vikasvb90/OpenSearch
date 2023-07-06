/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.SignaturePolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.encryption.blockdecryption.BlockDecryptionProvider;
import org.opensearch.encryption.blockdecryption.EncryptedHeaderContentSupplier;
import org.opensearch.encryption.core.AwsCrypto;
import org.opensearch.encryption.core.CryptoContext;
import org.opensearch.encryption.core.DecryptionHandler;
import org.opensearch.common.io.InputStreamContainer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CryptoTests {

    private static CryptoUtils cryptoUtils;

    private static CryptoUtils cryptoUtilsTrailingAlgo;

    @Before
    public void setupResources() {
        CryptoUtils.FRAME_SIZE = 100;
        MockKeyProvider keyProvider = new MockKeyProvider();
        CachingCryptoMaterialsManager cachingMaterialsManager = CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(keyProvider)
            .withCache(new LocalCryptoMaterialsCache(1000))
            .withMaxAge(10, TimeUnit.MINUTES)
            .build();

        AwsCrypto awsCrypto = new AwsCrypto(
            cachingMaterialsManager,
            CryptoUtils.FRAME_SIZE,
            CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384
        );
        AwsCrypto awsCryptoTrailingAlgo = new AwsCrypto(
            cachingMaterialsManager,
            CryptoUtils.FRAME_SIZE,
            CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384
        );
        cryptoUtils = new CryptoUtils(awsCrypto, new HashMap<>());
        cryptoUtilsTrailingAlgo = new CryptoUtils(awsCryptoTrailingAlgo, new HashMap<>());
    }

    static class EncryptedStore {
        byte[] encryptedContent;
        long rawLength;
        int encryptedLength;
        File file;
    }

    private EncryptedStore verifyAndGetEncryptedContent() throws IOException, URISyntaxException {
        return verifyAndGetEncryptedContent(false, cryptoUtils);
    }

    private EncryptedStore verifyAndGetEncryptedContent(boolean truncateRemainderPart, CryptoUtils cryptoUtils) throws IOException,
        URISyntaxException {
        String path = CryptoTests.class.getResource("/raw_content_for_crypto_test").toURI().getPath();
        File file = new File(path);

        Object cryptoContext = cryptoUtils.initCryptoContext();
        long length;
        byte[] encryptedContent = new byte[1024 * 20];
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            FileChannel channel = fileInputStream.getChannel();
            length = truncateRemainderPart ? channel.size() - (channel.size() % cryptoUtils.getFrameSize()) : channel.size();
        }

        int encLength = 0;
        try (OffsetRangeFileInputStream inputStream = new OffsetRangeFileInputStream(file.toPath(), length, 0)) {

            InputStreamContainer stream = new InputStreamContainer(inputStream, length, 0);
            InputStreamContainer encInputStream = cryptoUtils.createEncryptingStream(cryptoContext, stream);
            assertNotNull(encInputStream);

            int readBytes;
            while ((readBytes = encInputStream.getInputStream().read(encryptedContent, encLength, 1024)) != -1) {
                encLength += readBytes;
            }
        }

        long calculatedEncryptedLength = cryptoUtils.estimateEncryptedLength(cryptoContext, length);
        assertEquals(encLength, calculatedEncryptedLength);

        EncryptedStore encryptedStore = new EncryptedStore();
        encryptedStore.encryptedLength = encLength;
        encryptedStore.encryptedContent = encryptedContent;
        encryptedStore.rawLength = length;
        encryptedStore.file = file;
        return encryptedStore;
    }

    @Test
    public void testSingleStreamEncryption() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file);
        assertEquals(encryptedStore.rawLength, decryptedRawBytes);
    }

    @Test
    public void testSingleStreamEncryptionTrailingSignatureAlgo() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, cryptoUtilsTrailingAlgo);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file);
        assertEquals(encryptedStore.rawLength, decryptedRawBytes);
    }

    @Test
    public void testDecryptionOfCorruptedContent() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        encryptedStore.encryptedContent = "Corrupted content".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );

        Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );

    }

    private long decryptAndVerify(InputStream encryptedStream, long encSize, File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        long totalRawBytes = 0;
        try (FileChannel channel = inputStream.getChannel()) {
            channel.position(0);

            InputStream decryptingStream = cryptoUtils.createDecryptingStream(encryptedStream);
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] decryptedBuffer = new byte[1024];
                byte[] actualBuffer = new byte[1024];
                int readActualBytes;
                int readBytes;
                while ((readBytes = decryptingStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
                    readActualBytes = fis.read(actualBuffer, 0, actualBuffer.length);
                    assertEquals(readActualBytes, readBytes);
                    assertArrayEquals(actualBuffer, decryptedBuffer);
                    totalRawBytes += readActualBytes;
                }
            }
        }
        return totalRawBytes;
    }

    @Test
    public void testMultiPartStreamsEncryption() throws IOException, URISyntaxException {
        Object cryptoContextObj = cryptoUtils.initCryptoContext();
        CryptoContext cryptoContext = (CryptoContext) cryptoContextObj;
        String path = CryptoTests.class.getResource("/raw_content_for_crypto_test").toURI().getPath();
        File file = new File(path);
        byte[] encryptedContent = new byte[1024 * 20];
        int parts;
        long partSize, lastPartSize;
        long length;
        try (FileInputStream inputStream = new FileInputStream(file); FileChannel channel = inputStream.getChannel()) {
            length = channel.size();
        }
        partSize = getPartSize(length, cryptoUtils.getFrameSize());
        parts = numberOfParts(length, partSize);
        lastPartSize = length - (partSize * (parts - 1));

        int encLength = 0;
        for (int partNo = 0; partNo < parts; partNo++) {
            long size = partNo == parts - 1 ? lastPartSize : partSize;
            long pos = partNo * partSize;
            try (InputStream inputStream = getMultiPartStreamSupplier(file).apply(size, pos)) {
                InputStreamContainer rawStream = new InputStreamContainer(inputStream, size, pos);
                InputStreamContainer encStream = cryptoUtils.createEncryptingStreamOfPart(cryptoContextObj, rawStream, parts, partNo);
                int readBytes;
                int curEncryptedBytes = 0;
                while ((readBytes = encStream.getInputStream().read(encryptedContent, encLength, 1024)) != -1) {
                    encLength += readBytes;
                    curEncryptedBytes += readBytes;
                }
                assertEquals(encStream.getContentLength(), curEncryptedBytes);
            }
        }
        encLength += cryptoUtils.getTrailingSignatureLength(cryptoContext);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(encryptedContent, 0, encLength);
        decryptAndVerify(byteArrayInputStream, encLength, file);

    }

    private long getPartSize(long contentLength, int frameSize) {

        double optimalPartSizeDecimal = (double) contentLength / randomIntBetween(5, 10);
        // round up so we don't push the upload over the maximum number of parts
        long optimalPartSize = (long) Math.ceil(optimalPartSizeDecimal);
        if (optimalPartSize < frameSize) {
            optimalPartSize = frameSize;
        }

        if (optimalPartSize >= contentLength) {
            return contentLength;
        }

        if (optimalPartSize % frameSize > 0) {
            // When using encryption, parts must line up correctly along cipher block boundaries
            optimalPartSize = optimalPartSize - (optimalPartSize % frameSize) + frameSize;
        }
        return optimalPartSize;
    }

    private int numberOfParts(final long totalSize, final long partSize) {
        if (totalSize % partSize == 0) {
            return (int) (totalSize / partSize);
        }
        return (int) (totalSize / partSize) + 1;
    }

    private int randomIntBetween(int min, int max) {
        Random random = new Random();
        return random.nextInt(max - min) + min;
    }

    private BiFunction<Long, Long, CheckedInputStream> getMultiPartStreamSupplier(File localFile) {
        return (size, position) -> {
            OffsetRangeFileInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeFileInputStream(localFile.toPath(), size, position);
            } catch (IOException e) {
                return null;
            }
            return new CheckedInputStream(offsetRangeInputStream, new CRC32());
        };
    }

    @Test
    public void testBlockBasedDecryptionForEntireFile() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        assertTrue(
            "This test is meant for file size not exactly divisible by frame size",
            (encryptedStore.rawLength & cryptoUtils.getFrameSize()) != 0
        );
        validateBlockDownload(encryptedStore, 0, (int) encryptedStore.rawLength - 1);
    }

    @Test
    public void testBlockBasedDecryptionForEntireFileWithLinedUpFrameAlongFileBoundary() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(true, cryptoUtils);
        assertEquals(
            "This test is meant for file size exactly divisible by frame size",
            0,
            (encryptedStore.rawLength % cryptoUtils.getFrameSize())
        );
        validateBlockDownload(encryptedStore, 0, (int) encryptedStore.rawLength - 1);
    }

    @Test
    public void testCorruptedTrailingSignature() throws IOException, URISyntaxException {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, cryptoUtilsTrailingAlgo);
        byte[] trailingData = "corrupted".getBytes(StandardCharsets.UTF_8);
        byte[] corruptedTrailingContent = Arrays.copyOf(
            encryptedStore.encryptedContent,
            encryptedStore.encryptedContent.length + trailingData.length
        );
        System.arraycopy(trailingData, 0, corruptedTrailingContent, encryptedStore.encryptedContent.length, trailingData.length);
        encryptedStore.encryptedContent = corruptedTrailingContent;
        encryptedStore.encryptedLength = corruptedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    @Test
    public void testNoTrailingSignatureForTrailingAlgo() throws IOException, URISyntaxException {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, cryptoUtilsTrailingAlgo);
        Object cryptoContext = cryptoUtilsTrailingAlgo.initCryptoContext();
        int trailingLength = cryptoUtils.getTrailingSignatureLength(cryptoContext);
        byte[] removedTrailingContent = Arrays.copyOf(
            encryptedStore.encryptedContent,
            encryptedStore.encryptedContent.length - trailingLength
        );
        encryptedStore.encryptedContent = removedTrailingContent;
        encryptedStore.encryptedLength = removedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    @Test
    public void testOutputSizeEstimateWhenHandlerIsNull() {
        CryptoMaterialsManager cryptoMaterialsManager = Mockito.mock(CryptoMaterialsManager.class);
        DecryptionHandler<?> decryptionHandler = DecryptionHandler.create(
            cryptoMaterialsManager,
            CommitmentPolicy.RequireEncryptRequireDecrypt,
            SignaturePolicy.AllowEncryptAllowDecrypt,
            1
        );
        int inputLen = 50;
        int len = decryptionHandler.estimateOutputSize(inputLen);
        assertEquals(inputLen, len);
    }

    @Test
    public void testBlockBasedDecryptionForEntireFileWithoutPool() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        validateBlockDownload(encryptedStore, 0, (int) encryptedStore.rawLength - 1);
    }

    private EncryptedHeaderContentSupplier createEncryptedHeaderContentSupplier(byte[] encryptedContent) {
        return (start, end) -> {
            int len = (int) (end - start + 1);
            byte[] bytes = new byte[len];
            System.arraycopy(encryptedContent, (int) start, bytes, (int) start, len);
            return bytes;
        };
    }

    @Test
    public void testBlockBasedDecryptionForMiddleBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStore.rawLength / cryptoUtils.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStore,
            randomIntBetween(5, maxBlockNum / 2) * cryptoUtils.getFrameSize(),
            randomIntBetween(maxBlockNum / 2 + 1, maxBlockNum) * cryptoUtils.getFrameSize() - 1
        );
    }

    @Test
    public void testBlockBasedDecryptionForLastBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStore.rawLength / cryptoUtils.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStore,
            randomIntBetween(1, maxBlockNum - 1) * cryptoUtils.getFrameSize(),
            (int) encryptedStore.rawLength - 1
        );
    }

    private void validateBlockDownload(EncryptedStore encryptedStore, int startPos, int endPos) throws Exception {

        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier = createEncryptedHeaderContentSupplier(
            encryptedStore.encryptedContent
        );
        BlockDecryptionProvider decryptionSupplier = cryptoUtils.createBlockDecryptionProvider(encryptedHeaderContentSupplier);

        long[] transformedRange = decryptionSupplier.getTransformedEncryptedRange(new long[] { startPos, endPos });
        int encryptedBlockSize = (int) (transformedRange[1] - transformedRange[0] + 1);
        byte[] encryptedBlockBytes = new byte[encryptedBlockSize];
        System.arraycopy(encryptedStore.encryptedContent, (int) transformedRange[0], encryptedBlockBytes, 0, encryptedBlockSize);
        ByteArrayInputStream encryptedStream = new ByteArrayInputStream(encryptedBlockBytes, 0, encryptedBlockSize);
        InputStream decryptingStream = decryptionSupplier.provideBlockDecryptionStream(encryptedStream, startPos, endPos);

        decryptAndVerifyBlock(decryptingStream, encryptedStore.file, startPos, endPos);
    }

    @Test
    public void testBlockBasedDecryptionForFirstBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        // All block requests should properly line up with frames otherwise decryption will fail due to partial frames.
        int blockEnd = randomIntBetween(5, (int) encryptedStore.rawLength / cryptoUtils.getFrameSize()) * cryptoUtils.getFrameSize() - 1;
        validateBlockDownload(encryptedStore, 0, blockEnd);
    }

    @Test
    public void testBlockBasedDecryptionForCachedBlockProvider() throws IOException, URISyntaxException {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        AtomicBoolean headerFetched = new AtomicBoolean();
        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier = (start, end) -> {
            if (headerFetched.get()) {
                throw new IllegalStateException("Header fetch request received again. " + "Header should only be fetched once and cached");
            }
            int len = (int) (end - start + 1);
            byte[] bytes = new byte[len];
            System.arraycopy(encryptedStore.encryptedContent, (int) start, bytes, (int) start, len);
            headerFetched.set(true);
            return bytes;
        };

        BlockDecryptionProvider blockDecryptionProvider = cryptoUtils.createBlockDecryptionProvider(encryptedHeaderContentSupplier);

        // First block test
        int blockEnd = randomIntBetween(5, (int) encryptedStore.rawLength / cryptoUtils.getFrameSize()) * cryptoUtils.getFrameSize() - 1;
        validateBlockWithCachedEncryptedHeaders(encryptedStore, 0, blockEnd, blockDecryptionProvider);

        // Random middle block test
        int maxBlockNum = (int) encryptedStore.rawLength / cryptoUtils.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockWithCachedEncryptedHeaders(
            encryptedStore,
            randomIntBetween(1, maxBlockNum - 1) * cryptoUtils.getFrameSize(),
            (int) encryptedStore.rawLength - 1,
            blockDecryptionProvider
        );

        // Last block test
        validateBlockWithCachedEncryptedHeaders(
            encryptedStore,
            randomIntBetween(1, maxBlockNum - 1) * cryptoUtils.getFrameSize(),
            (int) encryptedStore.rawLength - 1,
            blockDecryptionProvider
        );
    }

    private void validateBlockWithCachedEncryptedHeaders(
        EncryptedStore encryptedStore,
        int startPos,
        int endPos,
        BlockDecryptionProvider blockDecryptionProvider
    ) throws IOException {

        long[] range = new long[] { startPos, endPos };
        long[] transformedRange = blockDecryptionProvider.getTransformedEncryptedRange(range);
        int encryptedBlockSize = (int) (transformedRange[1] - transformedRange[0] + 1);
        byte[] encryptedBlockBytes = new byte[encryptedBlockSize];
        System.arraycopy(encryptedStore.encryptedContent, (int) transformedRange[0], encryptedBlockBytes, 0, encryptedBlockSize);
        ByteArrayInputStream encryptedStream = new ByteArrayInputStream(encryptedBlockBytes, 0, encryptedBlockSize);
        InputStream decryptingStream = blockDecryptionProvider.provideBlockDecryptionStream(encryptedStream, startPos, endPos);

        decryptAndVerifyBlock(decryptingStream, encryptedStore.file, startPos, endPos);
    }

    private long decryptAndVerifyBlock(InputStream decryptedStream, File file, int rawContentStartPos, int rawContentEndPos)
        throws IOException {
        long totalRawBytes = 0;

        try (FileInputStream fis = new FileInputStream(file)) {
            fis.getChannel().position(rawContentStartPos);
            byte[] decryptedBuffer = new byte[100];
            byte[] actualBuffer = new byte[100];

            int readActualBytes;
            int readBytes;
            while ((readBytes = decryptedStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
                readActualBytes = fis.read(actualBuffer, 0, Math.min(actualBuffer.length, rawContentEndPos - rawContentStartPos + 1));
                rawContentEndPos -= readActualBytes;
                assertEquals(readActualBytes, readBytes);
                assertArrayEquals(actualBuffer, decryptedBuffer);
                totalRawBytes += readActualBytes;
            }
        }
        return totalRawBytes;
    }

    @Test
    public void testEmptyContentCrypto() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        Object cryptoContext = cryptoUtils.initCryptoContext();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = cryptoUtils.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = cryptoUtils.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

    @Test
    public void testEmptyContentCryptoTrailingSignatureAlgo() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        Object cryptoContext = cryptoUtilsTrailingAlgo.initCryptoContext();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = cryptoUtilsTrailingAlgo.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = cryptoUtilsTrailingAlgo.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

}
