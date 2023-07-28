package com.koi.javadb.backend.server;

import com.koi.javadb.backend.parser.statement.*;
import com.koi.javadb.backend.tbm.BeginRes;
import com.koi.javadb.backend.tbm.TableManager;
import com.koi.javadb.backend.parser.Parser;
import com.koi.javadb.common.Errors;

public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        // 解析sql，读取出第一个statement
        Object stat = Parser.Parse(sql);
        if (Begin.class.isInstance(stat)) {
            if (xid != 0) {
                throw Errors.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (Commit.class.isInstance(stat)) {
            if (xid == 0) {
                throw Errors.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (Abort.class.isInstance(stat)) {
            if (xid == 0) {
                throw Errors.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if (Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if (Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create) stat);
            } else if (Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select) stat);
            } else if (Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert) stat);
            } else if (Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete) stat);
            } else if (Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
