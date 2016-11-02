package com.sftxy.lucene.store.mongodb;

import java.util.ArrayList;

import org.junit.Test;

public class BootstrapTest {

    @Test
    public void testLock() throws Exception {
        String host = "localhost";
        String port = "55643";
        int size = 32;

        // Server
        new Thread(() -> {
            try {
                LockVerifyServer.main(host, port, String.valueOf(size));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Clients
        ArrayList<Thread> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int finalI = i;
            Thread thread = new Thread(() -> {
                try {
                    LockStressTest.main(String.valueOf(finalI), host, port, "com.sftxy.lucene.store.mongodb.MongodbLockFactory", "test", "1", "10000");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            list.add(thread);
            thread.start();
        }

        for (Thread t : list) {
            t.join();
        }
    }
}
