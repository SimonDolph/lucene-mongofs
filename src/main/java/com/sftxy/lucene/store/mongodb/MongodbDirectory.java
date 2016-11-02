package com.sftxy.lucene.store.mongodb;

import static com.mongodb.client.model.Filters.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;

/**
 * Mongodb based Lucene index directory.
 *
 * <p>This implementation use Mongodb GridFS to store index files.</p>
 * <p>About Mongodb GridFS see <a href="https://docs.mongodb.com/manual/core/gridfs/">GridFS</a></p>
 *
 * @see MongodbLockFactory
 */
public class MongodbDirectory extends BaseDirectory {

    private final MongoCollection<Document> locks;
    private final MongoCollection<Document> chunks;
    private final GridFSBucket fsBucket;
    private final String prefix;

    public MongodbDirectory(MongoDatabase db, String prefix) {
        super(MongodbLockFactory.INSTANCE);
        this.locks = db.getCollection("fs.locks");
        this.chunks = db.getCollection("fs.chunks");
        this.fsBucket = GridFSBuckets.create(db);
        this.prefix = prefix;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        List<String> items = new ArrayList<>();
        return fsBucket.find(regex("filename", Pattern.compile("^(?)" + prefix)))
                .map(GridFSFile::getFilename)
                .map(filename -> filename.substring(prefix.length() + 1))
                .into(items)
                .toArray(new String[items.size()]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        GridFSFile file = fsBucket.find(new Document("filename", prefix(name))).first();
        fsBucket.delete(file.getId());
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        GridFSFile file = fsBucket.find(new Document("filename", prefix(name))).first();
        return file.getLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return new MongodbIndexOutput(prefix(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // ignore
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        ensureOpen();
        GridFSFile file = fsBucket.find(new Document("filename", prefix(source))).first();
        fsBucket.rename(file.getId(), prefix(dest));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        GridFSFile file = fsBucket.find(new Document("filename", prefix(name))).first();
        return new MongodbIndexInput(file, chunks);
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
    }

    public void createLock(String lockName) {
        locks.insertOne(new Document("_id", prefix(lockName)));
    }

    public boolean checkLock(String lockName) {
        Document lock = locks.find(eq("_id", prefix(lockName))).first();
        return lock != null;
    }

    public boolean removeLock(String lockName) {
        DeleteResult result = locks.deleteOne(new Document("_id", prefix(lockName)));
        return result.getDeletedCount() == 1;
    }

    private String prefix(String name) {
        return prefix + "/" + name;
    }

    final class MongodbIndexOutput extends OutputStreamIndexOutput {
        public MongodbIndexOutput(String filename) {
            super("MongodbIndexOutput(filename=\"" + filename + "\")", fsBucket.openUploadStream(filename), 255);
        }
    }

}
