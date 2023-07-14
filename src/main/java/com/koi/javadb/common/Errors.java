package com.koi.javadb.common;

public class Errors {
    // common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    // tm
    public static final Exception BadXIDFieException = new RuntimeException("Bad XID file!");
}
