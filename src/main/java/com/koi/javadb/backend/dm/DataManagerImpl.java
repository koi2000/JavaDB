package com.koi.javadb.backend.dm;

import com.koi.javadb.backend.common.AbstractCache;
import com.koi.javadb.backend.dm.dataItem.DataItem;
import com.koi.javadb.backend.dm.dataItem.DataItemImpl;
import com.koi.javadb.backend.dm.logger.Logger;
import com.koi.javadb.backend.dm.page.Page;
import com.koi.javadb.backend.dm.page.PageOne;
import com.koi.javadb.backend.dm.page.PageX;
import com.koi.javadb.backend.dm.pageCache.PageCache;
import com.koi.javadb.backend.dm.pageIndex.PageIndex;
import com.koi.javadb.backend.dm.pageIndex.PageInfo;
import com.koi.javadb.backend.tm.TransactionManager;
import com.koi.javadb.backend.utils.Panic;
import com.koi.javadb.backend.utils.Types;
import com.koi.javadb.common.Errors;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    // 以uid为key从缓存框架中查出
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将数据封装一下，将size插入
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Errors.DataTooLargeException;
        }
        PageInfo pi = null;
        for (int i = 0; i < 5; i++) {
            // 根据数据的长度，找到freeSpace充足的页
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 否则创建新页
                int newPgNo = pc.newPage(PageX.initRaw());
                pIndex.add(newPgNo, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw Errors.DatabaseBusyException;
        }
        Page pg = null;
        int freeSpace = 0;
        try {
            // 读出页
            pg = pc.getPage(pi.pgno);
            // 生成日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 数据插入页中
            short offset = PageX.insert(pg, raw);
            // 刷盘
            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    // 检查第一页
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        // 将除了第一页的剩余Page都加入pIndex中
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
