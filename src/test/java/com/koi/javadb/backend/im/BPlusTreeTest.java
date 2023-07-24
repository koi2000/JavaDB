package com.koi.javadb.backend.im;

import com.koi.javadb.backend.dm.DataManager;
import com.koi.javadb.backend.dm.pageCache.PageCache;
import com.koi.javadb.backend.tm.MockTransactionManager;
import com.koi.javadb.backend.tm.TransactionManager;
import org.junit.Test;


import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for (int i = lim - 1; i >= 0; i--) {
            tree.insert(i, i);
        }

        for (int i = 0; i < lim; i++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }
        tree.close();
        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
