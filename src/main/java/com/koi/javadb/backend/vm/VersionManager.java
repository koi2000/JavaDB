package com.koi.javadb.backend.vm;

import com.koi.javadb.backend.dm.DataManager;
import com.koi.javadb.backend.tm.TransactionManager;

/**
 * @author koi
 * @date 2023/7/22 10:06
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid) throws Exception;

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
