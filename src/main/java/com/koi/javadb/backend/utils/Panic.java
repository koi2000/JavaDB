package com.koi.javadb.backend.utils;

public class Panic {
    public static void panic(Exception err){
        err.printStackTrace();
        System.exit(1);
    }

}
