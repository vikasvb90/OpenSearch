/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.crypto.CryptoManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

/**
 * The class responsible for orchestrating the transfer of a {@link TransferSnapshot} via a {@link TransferService}
 *
 * @opensearch.internal
 */
public class TranslogTransferManager {

    private final ShardId shardId;
    private final TransferService transferService;
    private final BlobPath remoteDataTransferPath;
    private final BlobPath remoteMetadataTransferPath;
    private final BlobPath remoteBaseTransferPath;
    private final FileTransferTracker fileTransferTracker;
    private final CryptoManager cryptoManager;

    private static final long TRANSFER_TIMEOUT_IN_MILLIS = 30000;

    private final Logger logger;
    private final static String METADATA_DIR = "metadata";
    private final static String DATA_DIR = "data";

    private static final VersionedCodecStreamWrapper<TranslogTransferMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new TranslogTransferMetadataHandler(),
        TranslogTransferMetadata.CURRENT_VERSION,
        TranslogTransferMetadata.METADATA_CODEC
    );

    public TranslogTransferManager(
        ShardId shardId,
        TransferService transferService,
        CryptoManager cryptoManager,
        BlobPath remoteBaseTransferPath,
        FileTransferTracker fileTransferTracker
    ) {
        this.cryptoManager = cryptoManager;
        this.shardId = shardId;
        this.transferService = transferService;
        this.remoteBaseTransferPath = remoteBaseTransferPath;
        this.remoteDataTransferPath = remoteBaseTransferPath.add(DATA_DIR);
        this.remoteMetadataTransferPath = remoteBaseTransferPath.add(METADATA_DIR);
        this.fileTransferTracker = fileTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public boolean transferSnapshot(TransferSnapshot transferSnapshot, TranslogTransferListener translogTransferListener)
        throws IOException {
        List<Exception> exceptionList = new ArrayList<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        Set<TransferFileSnapshot> toUpload = new HashSet<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        try {
            toUpload.addAll(fileTransferTracker.exclusionFilter(transferSnapshot.getTranslogFileSnapshots()));
            toUpload.addAll(fileTransferTracker.exclusionFilter((transferSnapshot.getCheckpointFileSnapshots())));
            if (toUpload.isEmpty()) {
                logger.trace("Nothing to upload for transfer");
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            }
            final CountDownLatch latch = new CountDownLatch(toUpload.size());
            LatchedActionListener<TransferFileSnapshot> latchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(fileTransferTracker::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Exception during transfer for file {}",
                            ((FileTransferException) ex).getFileSnapshot().getName()
                        ),
                        ex
                    );
                    FileTransferException e = (FileTransferException) ex;
                    fileTransferTracker.onFailure(e.getFileSnapshot(), ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            Map<Long, BlobPath> blobPathMap = new HashMap<>();
            toUpload.forEach(
                fileSnapshot -> blobPathMap.put(
                    fileSnapshot.getPrimaryTerm(),
                    remoteDataTransferPath.add(String.valueOf(fileSnapshot.getPrimaryTerm()))
                )
            );

            transferService.uploadBlobs(toUpload, blobPathMap, latchedActionListener, WritePriority.HIGH, TransferContentType.DATA);

            try {
                if (latch.await(TRANSFER_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS) == false) {
                    Exception ex = new TimeoutException("Timed out waiting for transfer of snapshot " + transferSnapshot + " to complete");
                    exceptionList.forEach(ex::addSuppressed);
                    throw ex;
                }
            } catch (InterruptedException ex) {
                exceptionList.forEach(ex::addSuppressed);
                Thread.currentThread().interrupt();
                throw ex;
            }
            if (exceptionList.isEmpty()) {
                transferService.uploadBlob(prepareMetadata(transferSnapshot), remoteMetadataTransferPath, WritePriority.HIGH, TransferContentType.METADATA);
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            } else {
                Exception ex = new IOException("Failed to upload " + exceptionList.size() + " files during transfer");
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Transfer failed for snapshot {}", transferSnapshot), ex);
            translogTransferListener.onUploadFailed(transferSnapshot, ex);
            return false;
        }
    }

    public boolean downloadTranslog(String primaryTerm, String generation, Path location) throws IOException {
        logger.info(
            "Downloading translog files with: Primary Term = {}, Generation = {}, Location = {}",
            primaryTerm,
            generation,
            location
        );
        // Download Checkpoint file from remote to local FS
        String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        downloadToFS(ckpFileName, location, primaryTerm, TransferContentType.DATA);
        // Download translog file from remote to local FS
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        downloadToFS(translogFilename, location, primaryTerm, TransferContentType.DATA);
        return true;
    }

    private void downloadToFS(String fileName, Path location, String primaryTerm, TransferContentType transferContentType)
        throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        // We need to change this logic when we introduce incremental download
        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }

        try (InputStream inputStream = transferService.downloadBlob(remoteDataTransferPath.add(primaryTerm), fileName)) {
            InputStream transferInputStream = inputStream;
            if (transferContentType == TransferContentType.DATA && cryptoManager != null) {
                transferInputStream = cryptoManager.createDecryptingStream(inputStream);
            }
            Files.copy(transferInputStream, filePath);
        }
        // Mark in FileTransferTracker so that the same files are not uploaded at the time of translog sync
        fileTransferTracker.add(fileName, true);
    }

    public TranslogTransferMetadata readMetadata() throws IOException {
        SetOnce<TranslogTransferMetadata> metadataSetOnce = new SetOnce<>();
        SetOnce<IOException> exceptionSetOnce = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<List<BlobMetadata>> latchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(blobMetadataList -> {
                if (blobMetadataList.isEmpty()) return;
                String filename = blobMetadataList.get(0).name();
                try (InputStream inputStream = transferService.downloadBlob(remoteMetadataTransferPath, filename)) {
                    IndexInput indexInput = new ByteArrayIndexInput("metadata file", inputStream.readAllBytes());
                    metadataSetOnce.set(metadataStreamWrapper.readStream(indexInput));
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("Exception while reading metadata file: {}", filename), e);
                    exceptionSetOnce.set(e);
                }
            }, e -> {
                logger.error(() -> new ParameterizedMessage("Exception while listing metadata files"), e);
                exceptionSetOnce.set((IOException) e);
            }),
            latch
        );

        try {
            transferService.listAllInSortedOrder(remoteMetadataTransferPath, 1, latchedActionListener);
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Exception while reading/downloading metadafile", e);
        }

        if (exceptionSetOnce.get() != null) {
            throw exceptionSetOnce.get();
        }

        return metadataSetOnce.get();
    }

    private TransferFileSnapshot prepareMetadata(TransferSnapshot transferSnapshot) throws IOException {
        Map<String, String> generationPrimaryTermMap = transferSnapshot.getTranslogFileSnapshots().stream().map(s -> {
            assert s instanceof TranslogFileSnapshot;
            return (TranslogFileSnapshot) s;
        })
            .collect(
                Collectors.toMap(
                    snapshot -> String.valueOf(snapshot.getGeneration()),
                    snapshot -> String.valueOf(snapshot.getPrimaryTerm())
                )
            );
        TranslogTransferMetadata translogTransferMetadata = transferSnapshot.getTranslogTransferMetadata();
        translogTransferMetadata.setGenerationToPrimaryTermMapper(new HashMap<>(generationPrimaryTermMap));

        return new TransferFileSnapshot(
            translogTransferMetadata.getFileName(),
            getMetadataBytes(translogTransferMetadata),
            translogTransferMetadata.getPrimaryTerm()
        );
    }

    /**
     * Get the metadata bytes for a {@link TranslogTransferMetadata} object
     *
     * @param metadata The object to be parsed
     * @return Byte representation for the given metadata
     */
    public byte[] getMetadataBytes(TranslogTransferMetadata metadata) throws IOException {
        byte[] metadataBytes;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "translog transfer metadata " + metadata.getPrimaryTerm(),
                    metadata.getFileName(),
                    output,
                    TranslogTransferMetadata.BUFFER_SIZE
                )
            ) {
                metadataStreamWrapper.writeStream(indexOutput, metadata);
            }
            metadataBytes = BytesReference.toBytes(output.bytes());
        }

        return metadataBytes;
    }

    /**
     * This method handles deletion of multiple generations for a single primary term. The deletion happens for translog
     * and metadata files.
     *
     * @param primaryTerm primary term where the generations will be deleted.
     * @param generations set of generation to delete.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    public void deleteGenerationAsync(long primaryTerm, Set<Long> generations, Runnable onCompletion) {
        List<String> translogFiles = new ArrayList<>();
        generations.forEach(generation -> {
            // Add .ckp and .tlog file to translog file list which is located in basePath/<primaryTerm>
            String ckpFileName = Translog.getCommitCheckpointFileName(generation);
            String translogFileName = Translog.getFilename(generation);
            translogFiles.addAll(List.of(ckpFileName, translogFileName));
        });
        // Delete the translog and checkpoint files asynchronously
        deleteTranslogFilesAsync(primaryTerm, translogFiles, onCompletion);
    }

    /**
     * Deletes all primary terms from remote store that are more than the given {@code minPrimaryTermToKeep}. The caller
     * of the method must ensure that the value is lesser than equal to the minimum primary term referenced by the remote
     * translog metadata.
     *
     * @param minPrimaryTermToKeep all primary terms below this primary term are deleted.
     */
    public void deletePrimaryTermsAsync(long minPrimaryTermToKeep) {
        logger.info("Deleting primary terms from remote store lesser than {}", minPrimaryTermToKeep);
        transferService.listFoldersAsync(ThreadPool.Names.REMOTE_PURGE, remoteDataTransferPath, new ActionListener<>() {
            @Override
            public void onResponse(Set<String> folders) {
                Set<Long> primaryTermsInRemote = folders.stream().filter(folderName -> {
                    try {
                        Long.parseLong(folderName);
                        return true;
                    } catch (Exception ignored) {
                        // NO-OP
                    }
                    return false;
                }).map(Long::parseLong).collect(Collectors.toSet());
                Set<Long> primaryTermsToDelete = primaryTermsInRemote.stream()
                    .filter(term -> term < minPrimaryTermToKeep)
                    .collect(Collectors.toSet());
                primaryTermsToDelete.forEach(term -> deletePrimaryTermAsync(term));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Exception occurred while getting primary terms from remote store", e);
            }
        });
    }

    /**
     * Handles deletion of all translog files associated with a primary term.
     *
     * @param primaryTerm primary term.
     */
    private void deletePrimaryTermAsync(long primaryTerm) {
        transferService.deleteAsync(
            ThreadPool.Names.REMOTE_PURGE,
            remoteDataTransferPath.add(String.valueOf(primaryTerm)),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info("Deleted primary term {}", primaryTerm);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(new ParameterizedMessage("Exception occurred while deleting primary term {}", primaryTerm), e);
                }
            }
        );
    }

    public void delete() {
        // cleans up all the translog contents in async fashion
        transferService.deleteAsync(ThreadPool.Names.REMOTE_PURGE, remoteBaseTransferPath, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("Deleted all remote translog data");
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Exception occurred while cleaning translog", e);
            }
        });
    }

    public void deleteStaleTranslogMetadataFilesAsync(Runnable onCompletion) {
        try {
            transferService.listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteMetadataTransferPath,
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        List<String> sortedMetadataFiles = blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList());
                        if (sortedMetadataFiles.size() <= 1) {
                            logger.trace("Remote Metadata file count is {}, so skipping deletion", sortedMetadataFiles.size());
                            onCompletion.run();
                            return;
                        }
                        List<String> metadataFilesToDelete = sortedMetadataFiles.subList(1, sortedMetadataFiles.size());
                        logger.trace("Deleting remote translog metadata files {}", metadataFilesToDelete);
                        deleteMetadataFilesAsync(metadataFilesToDelete, onCompletion);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Exception occurred while listing translog metadata files from remote store", e);
                        onCompletion.run();
                    }
                }
            );
        } catch (Exception e) {
            logger.error("Exception occurred while listing translog metadata files from remote store", e);
            onCompletion.run();
        }
    }

    public void deleteTranslogFiles() throws IOException {
        transferService.delete(remoteMetadataTransferPath);
        transferService.delete(remoteDataTransferPath);
    }

    /**
     * Deletes list of translog files asynchronously using the {@code REMOTE_PURGE} threadpool.
     *
     * @param primaryTerm primary term of translog files.
     * @param files       list of translog files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteTranslogFilesAsync(long primaryTerm, List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteDataTransferPath.add(String.valueOf(primaryTerm)),
                files,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        fileTransferTracker.delete(files);
                        logger.trace("Deleted translogs for primaryTerm={} files={}", primaryTerm, files);
                        onCompletion.run();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onCompletion.run();
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Exception occurred while deleting translog for primaryTerm={} files={}",
                                primaryTerm,
                                files
                            ),
                            e
                        );
                    }
                }
            );
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }

    /**
     * Deletes metadata files asynchronously using the {@code REMOTE_PURGE} threadpool. On success or failure, runs {@code onCompletion}.
     *
     * @param files list of metadata files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteMetadataFilesAsync(List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(ThreadPool.Names.REMOTE_PURGE, remoteMetadataTransferPath, files, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    onCompletion.run();
                    logger.trace("Deleted remote translog metadata files {}", files);
                }

                @Override
                public void onFailure(Exception e) {
                    onCompletion.run();
                    logger.error(new ParameterizedMessage("Exception occurred while deleting remote translog metadata files {}", files), e);
                }
            });
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }
}
