package com.koi.javadb.backend.tm;

import com.koi.javadb.backend.utils.Panic;
import com.koi.javadb.common.Errors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/*
 * 事务规则定义:
 * 每个事务都有一个XID，唯一标识了事务。
 * XID 0为一个超级事务，当一些操作想在没有申请事务的情况下进行，可以将其设为0
 * XID为 0 的事务的状态永远是committed
 * * * * * * * * * * */
public interface TransactionManager {
    // 开启一个新事物
    long begin();

    // 提交一个事务
    void commit(long xid);

    // 取消一个事务
    void abort(long xid);

    // 查询一个事务的状态是否是正在进行
    boolean isActive(long xid);

    // 查询一个事务的状态是否是已提交
    boolean isCommitted(long xid);

    // 查询一个事务的状态是否是已取消
    boolean isAborted(long xid);

    // 结束事务
    void close();

    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Errors.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Errors.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 持有两个文件操作相关的句柄
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Errors.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Errors.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
