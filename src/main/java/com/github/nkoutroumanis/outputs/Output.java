package com.github.nkoutroumanis.outputs;

public interface Output extends AutoCloseable {
    void out(String line, String lineMetaData);
}
