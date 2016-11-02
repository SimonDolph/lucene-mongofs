package com.sftxy.lucene.store.mongodb;

import java.io.IOException;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;

import com.mongodb.MongoWriteException;

/**
 * Mongodb based LockFactory.
 *
 * <p>This implementation keep lock in a Mongodb collection called "fs.locks", using lock name as _id in document.</p>
 */
public class MongodbLockFactory extends LockFactory {

    public static final MongodbLockFactory INSTANCE = new MongodbLockFactory();

    private MongodbLockFactory() {}

    @Override
    public Lock obtainLock(Directory dir0, String lockName) throws IOException {
        if (!(dir0 instanceof MongodbDirectory)) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " can only be used with MongodbDirectory");
        }
        MongodbDirectory dir = (MongodbDirectory) dir0;

        try {
            dir.createLock(lockName);
        } catch (MongoWriteException e) {
            throw new LockObtainFailedException("Lock held elsewhere: " + lockName, e);
        }

        return new MongodbLock(dir, lockName);
    }

    static final class MongodbLock extends Lock {

        private final MongodbDirectory dir;
        private final String lockName;

        private volatile boolean closed;

        MongodbLock(MongodbDirectory dir, String lockName) {
            this.dir = dir;
            this.lockName = lockName;
        }

        @Override
        public void ensureValid() throws IOException {
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }

            if (!dir.checkLock(lockName)) {
                throw new AlreadyClosedException("Underlying lock deleted by other programs: " + this);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }

            try {
                if (!dir.removeLock(lockName)) {
                    throw new LockReleaseFailedException("Lock already removed: " + this);
                }
            } catch (Throwable throwable) {
                throw new LockReleaseFailedException("Unable to remove lock: " + this);
            } finally {
                closed = true;
            }
        }
    }
}
