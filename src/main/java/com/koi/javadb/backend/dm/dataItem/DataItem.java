package com.koi.javadb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.koi.javadb.backend.common.SubArray;
import com.koi.javadb.backend.dm.DataManagerImpl;
import com.koi.javadb.backend.dm.page.Page;
import com.koi.javadb.backend.utils.Parser;
import com.koi.javadb.backend.utils.Types;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid,size,raw);
    }

    // 从页面offset处解析出dataItem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        // 从offset处开始，读出页面的中一个item的size
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        // 大小加上额外的三个字节
        short length = (short) (size + DataItemImpl.OF_DATA);
        // 读取出uid
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw,offset,offset+length),new byte[length],pg,uid,dm);
    }

    public static void setDataItemRawInvalid(byte[] raw){
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
