/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.util.concurrent.RunOnce;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OffsetRangeFileInputStream extends InputStream to read from a specified offset using FileChannel
 *
 * @opensearch.internal
 */
public class OffsetRangeFileInputStream extends OffsetRangeInputStream {
    private static final Logger logger = LogManager.getLogger(OffsetRangeIndexInputStream.class);
    private final InputStream inputStream;
    private final FileChannel fileChannel;

    private final long actualSizeToRead;
    // This is the maximum position till stream is to be read. If read methods exceed maxPos then bytes are read
    // till maxPos. If no byte is left after maxPos, then -1 is returned from read methods.
    private final long limit;
    // Position in stream from which read will start.
    private long counter = 0;

    private long markPointer;
    private long markCounter;
    private AtomicBoolean readBlock;
    private final OffsetRangeRefCount offsetRangeRefCount;
    private final RunOnce closeOnce;

    /**
     * Construct a new OffsetRangeFileInputStream object
     *
     * @param path Path to the file
     * @param size Number of bytes that need to be read from specified <code>position</code>
     * @param position Position from where the read needs to start
     * @throws IOException When <code>FileChannel#position</code> operation fails
     */
    public OffsetRangeFileInputStream(Path path, long size, long position) throws IOException {
        fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        fileChannel.position(position);
        inputStream = Channels.newInputStream(fileChannel);
        long totalLength = fileChannel.size();
        this.counter = 0;
        this.limit = size;
        if ((totalLength - position) > limit) {
            actualSizeToRead = limit;
        } else {
            actualSizeToRead = totalLength - position;
        }

        ClosingStreams closingStreams = new ClosingStreams(inputStream, fileChannel);
        offsetRangeRefCount = new OffsetRangeRefCount(closingStreams);
        closeOnce = new RunOnce(offsetRangeRefCount::decRef);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        ensureReadable();
        if (fileChannel.position() >= fileChannel.size()) {
            return -1;
        }
        if (fileChannel.position() + len > fileChannel.size()) {
            len = (int) (fileChannel.size() - fileChannel.position());
        }
        if (counter + len > limit) {
            len = (int) (limit - counter);
        }
        if (len <= 0) {
            return -1;
        }

        inputStream.read(b, off, len);
        counter += len;
        return len;
    }

    @Override
    public int read() throws IOException {
        if (counter++ >= limit) {
            return -1;
        }
        ensureReadable();
        return (fileChannel.position() < fileChannel.size()) ? (inputStream.read() & 0xff) : -1;
    }

    private void ensureReadable() {
        if (readBlock != null && readBlock.get() == true) {
            logger.debug("Read attempted on a stream which was read blocked!");
            throw alreadyClosed("Read blocked stream.");
        }
    }

    AlreadyClosedException alreadyClosed(String msg) {
        return new AlreadyClosedException(msg + this);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            markPointer = fileChannel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        markCounter = counter;
    }

    @Override
    public synchronized void reset() throws IOException {
        fileChannel.position(markPointer);
        counter = markCounter;
    }

    @Override
    public long getFilePointer() throws IOException {
        return fileChannel.position();
    }

    private static class ClosingStreams {
        private final InputStream inputStream;
        private final FileChannel channel;

        public ClosingStreams(InputStream inputStream, FileChannel channel) {
            this.inputStream = inputStream;
            this.channel = channel;
        }
    }

    private static class OffsetRangeRefCount extends RefCountedReleasable<ClosingStreams> {
        private static final Logger logger = LogManager.getLogger(OffsetRangeRefCount.class);

        public OffsetRangeRefCount(ClosingStreams ref) {
            super("OffsetRangeRefCount", ref, () -> {
                try {
                    ref.inputStream.close();
                } catch (IOException ex) {
                    logger.error("Failed to close inputstream", ex);
                }
                try {
                    ref.channel.close();
                } catch (IOException ex) {
                    logger.error("Failed to close channel", ex);
                }
            });
        }
    }

    @Override
    public void close() throws IOException {
        closeOnce.run();
    }
}
