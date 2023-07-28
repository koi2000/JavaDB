package com.koi.javadb.client;

import com.koi.javadb.backend.utils.Panic;
import com.koi.javadb.common.Errors;
import com.koi.javadb.transport.Encoder;
import com.koi.javadb.transport.Packager;
import com.koi.javadb.transport.Transporter;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
