package com.koi.javadb.backend.dm.pageCache;

import com.koi.javadb.backend.dm.page.Page;

public interface PageCache {

    int newPage(byte[] initData);

    Page getPage(int pageNo);

    void close();
}
