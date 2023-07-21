package com.koi.javadb.backend.dm.pageIndex;

public class PageInfo {
    // 页号
    public int pgno;
    // 页面剩余空间大小
    public int freeSpace;

    public PageInfo(int pgno,int freeSpace){
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
