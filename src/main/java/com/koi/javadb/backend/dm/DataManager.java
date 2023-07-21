package com.koi.javadb.backend.dm;

import com.koi.javadb.backend.dm.dataItem.DataItem;
import com.koi.javadb.backend.dm.logger.Logger;
import com.koi.javadb.backend.dm.page.PageOne;
import com.koi.javadb.backend.dm.pageCache.PageCache;
import com.koi.javadb.backend.dm.pageCache.PageCacheImpl;
import com.koi.javadb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long xid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long memory, TransactionManager tm) {
        PageCacheImpl pc = PageCache.create(path, memory);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long memory, TransactionManager tm) {
        PageCache pc = PageCache.open(path, memory);
        Logger lg = Logger.open(path);
        // 创建一个DataManager
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 检查第一页
        // 如果有问题，则进行recover
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        // 重新设置随机数
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
