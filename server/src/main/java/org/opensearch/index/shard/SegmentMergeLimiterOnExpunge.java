/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@ExperimentalApi
public class SegmentMergeLimiterOnExpunge {
    private static final Logger logger = Loggers.getLogger(SegmentMergeLimiterOnExpunge.class, "MergeLimiter");

    private final Set<String> segmentsToExpunge = new HashSet<>();
    private final Semaphore semaphore;
    private final Semaphore expungeSemaphore;
    private final Set<String> rejectedRegularMergeSegments = new HashSet<>();
    private volatile int dedicatedExpungePermits;
    private volatile int maxAllowedMerges;
    private volatile int updatedMaxAllowedMerges;
    private volatile Supplier<Boolean> aggressiveMergeDecision;

    public SegmentMergeLimiterOnExpunge(int maxAllowedMerges) {
        this.semaphore = new Semaphore(maxAllowedMerges, true);
        this.dedicatedExpungePermits = maxAllowedMerges / 2;
        this.expungeSemaphore = new Semaphore(this.dedicatedExpungePermits, true);
        this.maxAllowedMerges = maxAllowedMerges;
        this.updatedMaxAllowedMerges = maxAllowedMerges;
    }

    public void updateMaxAllowedMerges(int maxAllowedMerges) {
        this.updatedMaxAllowedMerges = maxAllowedMerges;
    }

    public void refreshPermits() {
        int updatedMaxMerges = this.updatedMaxAllowedMerges;
        int curMaxAllowedMerges = this.maxAllowedMerges;
        if (updatedMaxMerges != curMaxAllowedMerges) {
            int updatedDedicatedExpungePermits = updatedMaxMerges / 2;
            this.maxAllowedMerges = updatedMaxMerges;
            this.dedicatedExpungePermits = updatedDedicatedExpungePermits;
            resetSemaphore(semaphore, updatedMaxMerges);
            resetSemaphore(expungeSemaphore, updatedDedicatedExpungePermits);
        }
    }

    public synchronized Releasable acquireForDedicatedExpunges(Runnable onClose, Supplier<Boolean> aggressiveMergeDecision,
                                                               Set<String> newSegmentsToExpunge) throws InterruptedException {
        if (!this.segmentsToExpunge.isEmpty()) {
            throw new IllegalArgumentException("Background merges are already disabled");
        }

        if (updatedMaxAllowedMerges != maxAllowedMerges) {
            refreshPermits();
        }

        this.aggressiveMergeDecision = aggressiveMergeDecision;
        this.rejectedRegularMergeSegments.clear();
        this.segmentsToExpunge.addAll(newSegmentsToExpunge);
        this.semaphore.acquire(dedicatedExpungePermits);
        return Releasables.releaseOnce(() -> {
            this.segmentsToExpunge.clear();
            this.rejectedRegularMergeSegments.clear();
            this.semaphore.release(dedicatedExpungePermits);
            validatePermitsOnClose(semaphore, maxAllowedMerges, "Regular");
            validatePermitsOnClose(expungeSemaphore, dedicatedExpungePermits, "Expunge");
            onClose.run();
        });
    }

    private void validatePermitsOnClose(Semaphore semaphore, int maxPermits, String permitType) {
        if (semaphore.availablePermits() != maxPermits) {
            logger.error("{} merge semaphore has inconsistent state " +
                "max permits {} available permits {} ", permitType, maxPermits, semaphore.availablePermits());
            resetSemaphore(semaphore, maxPermits);
        }
    }

    private void resetSemaphore(Semaphore semaphore, int maxPermits) {
        semaphore.drainPermits();
        semaphore.release(maxPermits);
    }

    public boolean shouldRateLimitMerges() {
        return !segmentsToExpunge.isEmpty();
    }

    public synchronized Releasable acquirePermitForMerge(MergePolicy.OneMerge merge) {
        if (segmentsToExpunge.isEmpty() || merge == null) {
            return ()->{};
        }

        boolean isExpungeOp = false;
        Set<String> mergingSegments = new HashSet<>();
        boolean hasRejectedSegment = false;
        for (SegmentCommitInfo info : merge.segments) {
            mergingSegments.add(info.info.name);
            if (segmentsToExpunge.contains(info.info.name)) {
                isExpungeOp = true;
            }
            if (rejectedRegularMergeSegments.contains(info.info.name)) {
                hasRejectedSegment = true;
            }
        }

        Releasable mergePermit;
        try {
            boolean expungeAggressively = aggressiveMergeDecision.get();
            if (!isExpungeOp) {
                if (expungeAggressively) {
                    return null;
                }
                if ((mergePermit = acquirePermit(semaphore, 0)) != null) {
                    rejectedRegularMergeSegments.removeAll(mergingSegments);
                    if (hasRejectedSegment) {
                        logger.info("Removed segment merge {} from rejection", merge);
                    }
                } else {
                    logger.info("Rejected regular segment merge {}", merge);
                    rejectedRegularMergeSegments.addAll(mergingSegments);
                }
            } else {
                mergePermit = acquirePermit(expungeSemaphore, 0);
                if (mergePermit == null && (rejectedRegularMergeSegments.isEmpty() || expungeAggressively)) {
                    mergePermit = acquirePermit(semaphore, 1);
                    logger.info("Expunge permit out of capacity. Regular merge permit attempt " +
                        "{}, ", (mergePermit != null));
                } else {
                    logger.info("Expunge permit acquired");
                }
            }
        } catch (InterruptedException ex) {
            // If we get interrupted here then that can only happen in execution flow where expunges on segments are done
            // after acquireForDedicatedExpunges is invoked.
            logger.error("Failed to acquire permit for merge", ex);
            throw new RuntimeException(ex);
        }

        return mergePermit;
    }

    private Releasable acquirePermit(Semaphore semaphore, int durationInSecs) throws InterruptedException {
        Releasable mergePermit = null;
        if (semaphore.tryAcquire(1, durationInSecs, TimeUnit.SECONDS)) {
            final AtomicBoolean closed = new AtomicBoolean();
            mergePermit = () -> {
                if (closed.compareAndSet(false, true)) {
                    semaphore.release(1);
                }
            };
        }
        return mergePermit;
    }

    public MergePolicy.MergeSpecification excludeExpungingSegments(MergePolicy.MergeSpecification mergeSpecification) {
        MergePolicy.MergeSpecification updatedSpec = new MergePolicy.MergeSpecification();
        for (MergePolicy.OneMerge oneMerge : mergeSpecification.merges) {
            List<SegmentCommitInfo> updatedInfos = new ArrayList<>();
            for (SegmentCommitInfo info : oneMerge.segments) {
                if (!segmentsToExpunge.contains(info.info.name)) {
                    updatedInfos.add(info);
                }
            }
            if (updatedInfos.size() > 1) {
                updatedSpec.add(new MergePolicy.OneMerge(updatedInfos));
            }
        }
        return updatedSpec;
    }

    public MergePolicy.MergeSpecification createExpungeOnlySpec(SegmentInfos infos) {
        MergePolicy.MergeSpecification spec = new MergePolicy.MergeSpecification();
        List<String> merges = new ArrayList<>();
        for (SegmentCommitInfo info : infos) {
            if (segmentsToExpunge.contains(info.info.name)) {
                spec.add(new MergePolicy.OneMerge(Collections.singletonList(info)));
                merges.add(info.info.name);
            }
        }
        logger.info("Sending merges for deletes " + merges);
        return spec;
    }

}
