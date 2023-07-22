package com.koi.javadb.backend.vm;

import com.koi.javadb.backend.tm.TransactionManager;

/**
 * @author koi
 * @date 2023/7/22 16:51
 */
public class Visibility {
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 如果是创建的事务
        if (xmin == xid && xmax == 0) return true;
        // 创建的事务已提交
        if (tm.isCommitted(xmin)) {
            if (xmax == 0) return true;
            // 当前删除这个
            if (xmax != xid) {
                // 由一个未提交的事务删除
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if (xmin == xid && xmax == 0) return true;
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
