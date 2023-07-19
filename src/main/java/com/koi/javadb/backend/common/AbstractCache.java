package com.koi.javadb.backend.common;

import com.koi.javadb.common.Errors;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    // 实际缓存的数据
    private HashMap<Long, T> cache;
    // 元素的引用个数
    private HashMap<Long, Integer> references;
    // 正在获取某资源的线程
    private HashMap<Long, Boolean> getting;

    // 缓存的最大缓存资源数
    private int maxResource;
    // 缓存中元素的个数
    private int count;

    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            // 尝试获取该资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Errors.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        // 从缓存中拿不到
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        // 放入缓存
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    // 强行释放一个缓存
    protected void release(long key){
        lock.lock();
        try{
            int ref = references.get(key)-1;
            if (ref==0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    // 关闭缓存，写回所有资源
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    // 资源被驱逐写回
    protected abstract void releaseForCache(T obj);

    // 资源不在缓存时的获取行为
    protected abstract T getForCache(long key);

}
