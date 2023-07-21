package com.koi.javadb.backend.dm.logger;

import com.koi.javadb.backend.utils.Panic;
import com.koi.javadb.backend.utils.Parser;
import com.koi.javadb.common.Errors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/*
* 日志的二进制文件
* [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
* Size是一个四字节整数，标识Data段的字节数，Checksum 则是该条日志的校验和
* [Size][Checksum][Data]
* * * * * * * * */
public interface Logger {

    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    public static Logger create(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try{
            if (!f.createNewFile()){
                Panic.panic(Errors.FileExistsException);
            }
        }catch (IOException e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
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
        // 将校验和放进去，初始值为0
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!f.exists()){
            Panic.panic(Errors.FileExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
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
        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
