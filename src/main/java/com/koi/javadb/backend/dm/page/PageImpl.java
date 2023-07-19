package com.koi.javadb.backend.dm.page;

import com.koi.javadb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;

public class PageImpl implements Page{
    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;

    private PageCache pc;

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public byte[] getData() {
        return new byte[0];
    }
}
