/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.internal.crt.CopyRequestConversionUtils;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * An internal helper class that automatically uses multipart copy based on the size of the source object to copy
 * it to target remote directory. It is similar to #{@link software.amazon.awssdk.services.s3.internal.crt.CopyObjectHelper}
 * with some changes to retain part ranges and object metadata.
 */
@SuppressWarnings("rawtypes")
public final class CopyObjectHelper {
    private static final Logger log = LogManager.getLogger(AsyncPartsHandler.class);
    private final S3AsyncClient s3AsyncClient;

    public CopyObjectHelper(S3AsyncClient s3AsyncClient) {
        this.s3AsyncClient = s3AsyncClient;
    }

    public CopyObjectMetadata fetchCopyObjectMetadata(String sourceBucket, String sourceKey) {

        CopyObjectMetadata copyObjectMetadata = new CopyObjectMetadata();
        fetchPartCountAndMetadata(sourceBucket, sourceKey, copyObjectMetadata);
        if (copyObjectMetadata.getPartCount() != null) {
            fetchPartSizes(copyObjectMetadata, sourceBucket, sourceKey);
        }
        return copyObjectMetadata;
    }

    private void fetchPartCountAndMetadata(String sourceBucket, String sourceKey,
                                           CopyObjectMetadata copyObjectMetadata) {
        GetObjectAttributesRequest attributesRequest = GetObjectAttributesRequest.builder()
            .bucket(sourceBucket)
            .key(sourceKey)
            .objectAttributes(ObjectAttributes.OBJECT_PARTS)
            .build();
        CompletableFuture<GetObjectAttributesResponse> partCountFuture = s3AsyncClient.getObjectAttributes(attributesRequest);
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
            .bucket(sourceBucket)
            .key(sourceKey)
            .build();
        CompletableFuture<HeadObjectResponse> headObjectFuture = s3AsyncClient.headObject(headObjectRequest);
        CompletableFuture<Void> partAndMetadataFuture = CompletableFuture.allOf(partCountFuture, headObjectFuture)
            .thenApply(__ -> {
                GetObjectAttributesResponse partCountResp = partCountFuture.join();
                Integer partsCount = null;
                if (partCountResp.objectParts() != null) {
                    partsCount = partCountResp.objectParts().totalPartsCount();
                }
                HeadObjectResponse metadataResp = headObjectFuture.join();
                copyObjectMetadata.setPartCount(partsCount);
                copyObjectMetadata.setMetadata(metadataResp.metadata());
                return null;
            });

        partAndMetadataFuture.join();
    }

    private void fetchPartSizes(CopyObjectMetadata copyObjectMetadata,
                                String sourceBucket, String sourceKey) {
        List<CompletableFuture<HeadObjectResponse>> headFutures = Collections.synchronizedList(new ArrayList<>());
        for (int part = 1; part <= copyObjectMetadata.getPartCount(); part++) {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .partNumber(part)
                .bucket(sourceBucket)
                .key(sourceKey)
                .build();
            CompletableFuture<HeadObjectResponse> headRespFuture = s3AsyncClient.headObject(headObjectRequest);
            headFutures.add(headRespFuture);
        }
        CompletableFuture<Void> respFuture = CompletableFuture.allOf(headFutures.toArray(new CompletableFuture[0]))
            .thenApply(__ -> {
                Map<Integer, Long> partSizes = new HashMap<>();
                for (int partIdx = 0; partIdx < headFutures.size(); partIdx++) {
                    HeadObjectResponse headObjectResponse = headFutures.get(partIdx).join();
                    partSizes.put(partIdx + 1, headObjectResponse.contentLength());
                }
                copyObjectMetadata.setPartSizes(partSizes);
                return null;
            });

        respFuture.join();
    }

    public CompletableFuture<Void> copyObject(String sourceBucket, String sourceKey, CopyObjectMetadata copyObjectMetadata,
                                              String destinationBucket, String destinationKey) {
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
            .sourceBucket(sourceBucket)
            .sourceKey(sourceKey)
            .destinationBucket(destinationBucket)
            .destinationKey(destinationKey)
            .metadata(copyObjectMetadata.getMetadata())
            .build();

        CompletableFuture<Void> returnFuture = new CompletableFuture<>();
        try {
            if (copyObjectMetadata.getPartCount() == null) {
                log.debug(() -> "Starting the copy as a single copy part request");
                copyInOneChunk(copyObjectRequest, returnFuture);
            } else {
                log.debug(() -> "Starting the copy as multipart copy request");
                copyInParts(copyObjectRequest, returnFuture, copyObjectMetadata);
            }
        } catch (Throwable throwable) {
            returnFuture.completeExceptionally(throwable);
        }
        return returnFuture;
    }

    private void copyInParts(CopyObjectRequest copyObjectRequest,
                             CompletableFuture<Void> returnFuture,
                             CopyObjectMetadata copyObjectMetadata) {
        CreateMultipartUploadRequest request = CopyRequestConversionUtils.toCreateMultipartUploadRequest(copyObjectRequest);
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadFuture =
            s3AsyncClient.createMultipartUpload(request);

        // Ensure cancellations are forwarded to the createMultipartUploadFuture future
        CompletableFutureUtils.forwardExceptionTo(returnFuture, createMultipartUploadFuture);

        createMultipartUploadFuture.whenComplete((createMultipartUploadResponse, throwable) -> {
            if (throwable != null) {
                handleException(returnFuture, () -> "Failed to initiate multipart copy", throwable);
            } else {
                log.debug(() -> "Initiated new multipart copy, uploadId: " + createMultipartUploadResponse.uploadId());
                doCopyInParts(copyObjectRequest, returnFuture, createMultipartUploadResponse.uploadId(),
                    copyObjectMetadata);
            }
        });
    }

    public void handleException(CompletableFuture<Void> returnFuture,
                                Supplier<String> message,
                                Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        if (cause instanceof Error || cause instanceof SdkException) {
            cause.addSuppressed(SdkClientException.create(message.get()));
            returnFuture.completeExceptionally(cause);
        } else {
            SdkClientException exception = SdkClientException.create(message.get(), cause);
            returnFuture.completeExceptionally(exception);
        }
    }

    private void doCopyInParts(CopyObjectRequest copyObjectRequest,
                               CompletableFuture<Void> returnFuture,
                               String uploadId,
                               CopyObjectMetadata copyObjectMetadata) {

        log.debug(() -> String.format("Starting multipart copy with partCount: %s", copyObjectMetadata.getPartCount()));

        // The list of completed parts must be sorted
        AtomicReferenceArray<CompletedPart> completedParts = new AtomicReferenceArray<>(copyObjectMetadata.getPartCount());

        List<CompletableFuture<CompletedPart>> futures = sendUploadPartCopyRequests(copyObjectRequest,
            uploadId,
            completedParts,
            copyObjectMetadata);
        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(new CompletableFuture[0]))
            .thenCompose(ignore -> completeMultipartUpload(copyObjectRequest, uploadId, completedParts))
            .handle(handleExceptionOrResponse(copyObjectRequest, returnFuture, uploadId))
            .exceptionally(throwable -> {
                handleException(returnFuture, () -> "Unexpected exception occurred", throwable);
                return null;
            });
    }

    private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
        CopyObjectRequest copyObjectRequest, String uploadId, AtomicReferenceArray<CompletedPart> completedParts) {
        log.debug(() -> String.format("Sending completeMultipartUploadRequest, copyId: %s",
            uploadId));
        CompletedPart[] parts =
            IntStream.range(0, completedParts.length())
                .mapToObj(completedParts::get)
                .toArray(CompletedPart[]::new);
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            CompleteMultipartUploadRequest.builder()
                .bucket(copyObjectRequest.destinationBucket())
                .key(copyObjectRequest.destinationKey())
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                    .parts(parts)
                    .build())
                .build();
        return s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest);
    }


    private List<CompletableFuture<CompletedPart>> sendUploadPartCopyRequests(CopyObjectRequest copyObjectRequest,
                                                                              String uploadId,
                                                                              AtomicReferenceArray<CompletedPart> completedParts,
                                                                              CopyObjectMetadata copyObjectMetadata) {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
        List<UploadPartCopyRequest> uploadPartCopyRequests = new ArrayList<>(copyObjectMetadata.getPartCount());
        long offset = 0;
        for (int part = 1; part <= copyObjectMetadata.getPartCount(); part++) {
            long partSize = copyObjectMetadata.getPartSizes().get(part);
            UploadPartCopyRequest uploadPartCopyRequest = UploadPartCopyRequest.builder()
                .sourceBucket(copyObjectRequest.sourceBucket())
                .sourceKey(copyObjectRequest.sourceKey())
                .uploadId(uploadId)
                .copySourceRange(range(partSize, offset))
                .partNumber(part)
                .destinationBucket(copyObjectRequest.destinationBucket())
                .destinationKey(copyObjectRequest.destinationKey())
                .build();
            uploadPartCopyRequests.add(uploadPartCopyRequest);
            offset += partSize;
        }

        uploadPartCopyRequests.forEach(uploadPartCopyRequest ->
            sendIndividualUploadPartCopy(uploadId, completedParts, futures,
                uploadPartCopyRequest));

        return futures;
    }

    private void sendIndividualUploadPartCopy(String uploadId,
                                              AtomicReferenceArray<CompletedPart> completedParts,
                                              List<CompletableFuture<CompletedPart>> futures,
                                              UploadPartCopyRequest uploadPartCopyRequest) {
        Integer partNumber = uploadPartCopyRequest.partNumber();
        log.debug(() -> "Sending copyPartCopyRequest with range: " + uploadPartCopyRequest.copySourceRange() + " uploadId: "
            + uploadId);

        CompletableFuture<UploadPartCopyResponse> uploadPartCopyFuture =
            s3AsyncClient.uploadPartCopy(uploadPartCopyRequest);

        CompletableFuture<CompletedPart> convertFuture =
            uploadPartCopyFuture.thenApply(uploadPartCopyResponse ->
                convertUploadPartCopyResponse(completedParts, partNumber, uploadPartCopyResponse));
        futures.add(convertFuture);

        CompletableFutureUtils.forwardExceptionTo(convertFuture, uploadPartCopyFuture);
    }

    private void copyInOneChunk(CopyObjectRequest copyObjectRequest,
                                CompletableFuture<Void> returnFuture) {
        CompletableFuture<Void> copyObjectFuture =
            s3AsyncClient.copyObject(copyObjectRequest).thenApply(resp -> null);
        CompletableFutureUtils.forwardExceptionTo(returnFuture, copyObjectFuture);
        CompletableFutureUtils.forwardResultTo(copyObjectFuture, returnFuture);
    }

    private BiFunction<CompleteMultipartUploadResponse, Throwable, Void> handleExceptionOrResponse(
        CopyObjectRequest copyObjectRequest,
        CompletableFuture<Void> returnFuture,
        String uploadId) {

        return (completeMultipartUploadResponse, throwable) -> {
            if (throwable != null) {
                cleanUpParts(copyObjectRequest, uploadId);
                handleException(returnFuture, () -> "Failed to send multipart copy requests.",
                    throwable);
            } else {
                returnFuture.complete(null);
            }

            return null;
        };
    }
    private void cleanUpParts(CopyObjectRequest copyObjectRequest, String uploadId) {
        AbortMultipartUploadRequest abortMultipartUploadRequest =
            CopyRequestConversionUtils.toAbortMultipartUploadRequest(copyObjectRequest, uploadId);
        s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest)
            .exceptionally(throwable -> {
                log.warn(() -> String.format("Failed to abort previous multipart upload (id: %s)", uploadId), throwable);
                return null;
            });
    }

    public static String range(long partSize, long offset) {
        return "bytes=" + offset + "-" + (offset + partSize - 1);
    }

    private static CompletedPart convertUploadPartCopyResponse(AtomicReferenceArray<CompletedPart> completedParts,
                                                               Integer partNumber,
                                                               UploadPartCopyResponse uploadPartCopyResponse) {
        CopyPartResult copyPartResult = uploadPartCopyResponse.copyPartResult();
        CompletedPart completedPart = CopyRequestConversionUtils.toCompletedPart(copyPartResult, partNumber);

        completedParts.set(partNumber - 1, completedPart);
        return completedPart;
    }
}
