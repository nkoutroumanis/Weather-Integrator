package com.github.nkoutroumanis.outputs;

public interface Output<T> extends AutoCloseable {
    void out(T data, String metaData);
}
