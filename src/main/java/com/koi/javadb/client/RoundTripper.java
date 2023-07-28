package com.koi.javadb.client;

import com.koi.javadb.transport.Package;
import com.koi.javadb.transport.Packager;

public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager){
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
