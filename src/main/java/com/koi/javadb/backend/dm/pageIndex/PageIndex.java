package com.koi.javadb.backend.dm.pageIndex;

import com.koi.javadb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 根据不同大小划分到不同区间
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 每个区间的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        // 每个区间维护一个List
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 共有多少个区间
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 找到所属的大小区间
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            //
            while (number <= INTERVALS_NO) {
                // 如果这个区间没有元素
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
