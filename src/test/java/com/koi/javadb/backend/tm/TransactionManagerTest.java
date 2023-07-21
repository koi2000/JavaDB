package com.koi.javadb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author koi
 * @date 2023/7/14 23:44
 */
public class TransactionManagerTest {
    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        tmger = TransactionManager.create("/tmp/tranmger_test01");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tmger.close();
        assert new File("/tmp/tranmger_test01.xid").delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for (int i = 0; i < noWorks; i++) {
            int op = Math.abs(random.nextInt(6));
            if (op == 0) {
                lock.lock();
                if (inTrans == false) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte) 0);
                    transCnt++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte) status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                if (transCnt > 0) {
                    long xid = (long) ((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
