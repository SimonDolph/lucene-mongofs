package com.sftxy.lucene.store.mongodb;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * Inspired by Lucene's {@link org.apache.lucene.store.ByteBufferIndexInput}.
 */
abstract class ByteBufferIndexInput extends IndexInput implements RandomAccessInput {
    protected final long length;
    protected final int chunkSize;

    protected ByteBuffer[] buffers;
    protected int curBufIndex = -1;
    protected ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]

    protected boolean isClone = false;

    public static ByteBufferIndexInput newInstance(String resourceDescription, ByteBuffer[] buffers, long length, int chunkSize) {
        if (buffers.length == 1) {
            return new SingleBufferImpl(resourceDescription, buffers[0], length, chunkSize);
        } else {
            return new MultiBufferImpl(resourceDescription, buffers, 0, length, chunkSize);
        }
    }

    ByteBufferIndexInput(String resourceDescription, ByteBuffer[] buffers, long length, int chunkSize) {
        super(resourceDescription);
        this.buffers = buffers;
        this.length = length;
        this.chunkSize = chunkSize;
    }

    @Override
    public final byte readByte() throws IOException {
        try {
            return curBuf.get();
        } catch (BufferUnderflowException e) {
            do {
                curBufIndex++;
                if (curBufIndex >= buffers.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                curBuf = buffers[curBufIndex];
                curBuf.position(0);
            } while (!curBuf.hasRemaining());
            return curBuf.get();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        try {
            curBuf.get(b, offset, len);
        } catch (BufferUnderflowException e) {
            int curAvail = curBuf.remaining();
            while (len > curAvail) {
                curBuf.get(b, offset, curAvail);
                len -= curAvail;
                offset += curAvail;
                curBufIndex++;
                if (curBufIndex >= buffers.length) {
                    throw new EOFException("read past EOF: " + this);
                }
                curBuf = buffers[curBufIndex];
                curBuf.position(0);
                curAvail = curBuf.remaining();
            }
            curBuf.get(b, offset, len);
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public final short readShort() throws IOException {
        try {
            return curBuf.getShort();
        } catch (BufferUnderflowException e) {
            return super.readShort();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public final int readInt() throws IOException {
        try {
            return curBuf.getInt();
        } catch (BufferUnderflowException e) {
            return super.readInt();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public final long readLong() throws IOException {
        try {
            return curBuf.getLong();
        } catch (BufferUnderflowException e) {
            return super.readLong();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public long getFilePointer() {
        try {
            return curBufIndex * chunkSize + curBuf.position();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        // we use >> here to preserve negative, so we will catch AIOOBE,
        // in case pos + offset overflows.
        final int bi = (int) (pos / chunkSize);
        try {
            if (bi == curBufIndex) {
                curBuf.position((int) (pos % chunkSize));
            } else {
                final ByteBuffer b = buffers[bi];
                b.position((int) (pos % chunkSize));
                // write values, on exception all is unchanged
                this.curBufIndex = bi;
                this.curBuf = b;
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException e) {
            throw new EOFException("seek past EOF: " + this);
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public byte readByte(long pos) throws IOException {
        try {
            final int bi = (int) (pos / chunkSize);
            return buffers[bi].get((int) (pos % chunkSize));
        } catch (IndexOutOfBoundsException ioobe) {
            throw new EOFException("seek past EOF: " + this);
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    // used only by random access methods to handle reads across boundaries
    private void setPos(long pos, int bi) throws IOException {
        try {
            final ByteBuffer b = buffers[bi];
            b.position((int) (pos % chunkSize));
            this.curBufIndex = bi;
            this.curBuf = b;
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException aioobe) {
            throw new EOFException("seek past EOF: " + this);
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        final int bi = (int) (pos / chunkSize);
        try {
            return buffers[bi].getShort((int) (pos % chunkSize));
        } catch (IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, bi);
            return readShort();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        final int bi = (int) (pos / chunkSize);
        try {
            return buffers[bi].getInt((int) (pos % chunkSize));
        } catch (IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, bi);
            return readInt();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        final int bi = (int) (pos / chunkSize);
        try {
            return buffers[bi].getLong((int) (pos % chunkSize));
        } catch (IndexOutOfBoundsException ioobe) {
            // either it's a boundary, or read past EOF, fall back:
            setPos(pos, bi);
            return readLong();
        } catch (NullPointerException npe) {
            throw new AlreadyClosedException("Already closed: " + this);
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public final ByteBufferIndexInput clone() {
        final ByteBufferIndexInput clone = buildSlice((String) null, 0L, this.length);
        try {
            clone.seek(getFilePointer());
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }

        return clone;
    }

    /**
     * Creates a slice of this index input, with the given description, offset, and length. The slice is seeked to the beginning.
     */
    @Override
    public final ByteBufferIndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" + this.length + ": " + this);
        }

        return buildSlice(sliceDescription, offset, length);
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    protected ByteBufferIndexInput buildSlice(String sliceDescription, long offset, long length) {
        if (buffers == null) {
            throw new AlreadyClosedException("Already closed: " + this);
        }

        final ByteBuffer newBuffers[] = buildSlice(buffers, offset, length);
        final int ofs = (int) (offset % chunkSize);

        final ByteBufferIndexInput clone = newCloneInstance(getFullSliceDescription(sliceDescription), newBuffers, ofs, length);
        clone.isClone = true;

        return clone;
    }

    /**
     * Factory method that creates a suitable implementation of this class for the given ByteBuffers.
     */
    @SuppressWarnings("resource")
    protected ByteBufferIndexInput newCloneInstance(String newResourceDescription, ByteBuffer[] newBuffers, int offset, long length) {
        if (newBuffers.length == 1) {
            newBuffers[0].position(offset);
            return new SingleBufferImpl(newResourceDescription, newBuffers[0].slice(), length, chunkSize);
        } else {
            return new MultiBufferImpl(newResourceDescription, newBuffers, offset, length, chunkSize);
        }
    }

    /**
     * Returns a sliced view from a set of already-existing buffers:
     * the last buffer's limit() will be correct, but
     * you must deal with offset separately (the first buffer will not be adjusted)
     */
    private ByteBuffer[] buildSlice(ByteBuffer[] buffers, long offset, long length) {
        final long sliceEnd = offset + length;

        final int startIndex = (int) (offset / chunkSize);
        final int endIndex = (int) (sliceEnd / chunkSize);

        // we always allocate one more slice, the last one may be a 0 byte one
        final ByteBuffer slices[] = new ByteBuffer[endIndex - startIndex + 1];

        for (int i = 0; i < slices.length; i++) {
            slices[i] = buffers[startIndex + i].duplicate();
        }

        // set the last buffer's limit for the sliced view.
        slices[slices.length - 1].limit((int) (sliceEnd % chunkSize));

        return slices;
    }

    @Override
    public final void close() throws IOException {
        if (buffers == null) return;
        unsetBuffers();
    }

    /**
     * Called to remove all references to byte buffers, so we can throw AlreadyClosed on NPE.
     */
    private void unsetBuffers() {
        buffers = null;
        curBuf = null;
        curBufIndex = 0;
    }

    /**
     * Optimization of ByteBufferIndexInput for when there is only one buffer
     */
    static final class SingleBufferImpl extends ByteBufferIndexInput {

        SingleBufferImpl(String resourceDescription, ByteBuffer buffer, long length, int chunkSize) {
            super(resourceDescription, new ByteBuffer[]{buffer}, length, chunkSize);
            this.curBufIndex = 0;
            this.curBuf = buffer;
            buffer.position(0);
        }

        // TODO: investigate optimizing readByte() & Co?

        @Override
        public void seek(long pos) throws IOException {
            try {
                curBuf.position((int) pos);
            } catch (IllegalArgumentException e) {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this, e);
                } else {
                    throw new EOFException("seek past EOF: " + this);
                }
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

        @Override
        public long getFilePointer() {
            try {
                return curBuf.position();
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

        @Override
        public byte readByte(long pos) throws IOException {
            try {
                return curBuf.get((int) pos);
            } catch (IllegalArgumentException e) {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this, e);
                } else {
                    throw new EOFException("seek past EOF: " + this);
                }
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

        @Override
        public short readShort(long pos) throws IOException {
            try {
                return curBuf.getShort((int) pos);
            } catch (IllegalArgumentException e) {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this, e);
                } else {
                    throw new EOFException("seek past EOF: " + this);
                }
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

        @Override
        public int readInt(long pos) throws IOException {
            try {
                return curBuf.getInt((int) pos);
            } catch (IllegalArgumentException e) {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this, e);
                } else {
                    throw new EOFException("seek past EOF: " + this);
                }
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

        @Override
        public long readLong(long pos) throws IOException {
            try {
                return curBuf.getLong((int) pos);
            } catch (IllegalArgumentException e) {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this, e);
                } else {
                    throw new EOFException("seek past EOF: " + this);
                }
            } catch (NullPointerException npe) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }
    }

    /**
     * This class adds offset support to ByteBufferIndexInput, which is needed for slices.
     */
    static final class MultiBufferImpl extends ByteBufferIndexInput {
        private final int offset;

        MultiBufferImpl(String resourceDescription, ByteBuffer[] buffers, int offset, long length, int chunkSize) {
            super(resourceDescription, buffers, length, chunkSize);
            this.offset = offset;
            try {
                seek(0L);
            } catch (IOException ioe) {
                throw new AssertionError(ioe);
            }
        }

        @Override
        public void seek(long pos) throws IOException {
            assert pos >= 0L;
            super.seek(pos + offset);
        }

        @Override
        public long getFilePointer() {
            return super.getFilePointer() - offset;
        }

        @Override
        public byte readByte(long pos) throws IOException {
            return super.readByte(pos + offset);
        }

        @Override
        public short readShort(long pos) throws IOException {
            return super.readShort(pos + offset);
        }

        @Override
        public int readInt(long pos) throws IOException {
            return super.readInt(pos + offset);
        }

        @Override
        public long readLong(long pos) throws IOException {
            return super.readLong(pos + offset);
        }

        @Override
        protected ByteBufferIndexInput buildSlice(String sliceDescription, long ofs, long length) {
            return super.buildSlice(sliceDescription, this.offset + ofs, length);
        }
    }
}
