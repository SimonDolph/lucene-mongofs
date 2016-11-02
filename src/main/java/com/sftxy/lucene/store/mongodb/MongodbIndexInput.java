package com.sftxy.lucene.store.mongodb;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Mongodb based IndexInput.
 *
 * <p>During creation, fetch all data from fs.chunks to construct {@link ByteBufferIndexInput}.</p>
 *
 * <p>After creation, delegate calls to {@link ByteBufferIndexInput}.</p>
 */
public class MongodbIndexInput extends IndexInput implements RandomAccessInput {

    private final ByteBufferIndexInput indexInput;

    MongodbIndexInput(GridFSFile file, MongoCollection<Document> chunks) {
        super("MongodbIndexInput(file=\"" + file + "\")");

        List<ByteBuffer> bufferList = new ArrayList<>();
        ByteBuffer[] buffers = chunks.find(and(eq("files_id", file.getId())))
            .sort(ascending("n"))
            .map(doc -> doc.get("data", Binary.class).getData())
            .map(ByteBuffer::wrap)
            .into(bufferList)
            .toArray(new ByteBuffer[bufferList.size()]);

        long length = file.getLength();
        int chunkSize = file.getChunkSize();

        indexInput = ByteBufferIndexInput.newInstance("MongodbIndexInput$Delegate(file=\"" + file + "\")", buffers, length, chunkSize);
    }

    @Override
    public void close() throws IOException {
        indexInput.close();
    }

    @Override
    public long getFilePointer() {
        return indexInput.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        indexInput.seek(pos);
    }

    @Override
    public long length() {
        return indexInput.length();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return indexInput.slice(sliceDescription, offset, length);
    }

    @Override
    public byte readByte() throws IOException {
        return indexInput.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        indexInput.readBytes(b, offset, len);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return indexInput.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return indexInput.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return indexInput.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return indexInput.readLong(pos);
    }
}
