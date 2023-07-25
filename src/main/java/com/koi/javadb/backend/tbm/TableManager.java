package com.koi.javadb.backend.tbm;

import com.koi.javadb.backend.parser.statement.Create;

public interface TableManager {

    BeginRes begin();
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create);


}
