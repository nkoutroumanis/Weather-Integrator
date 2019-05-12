package com.github.nkoutroumanis.outputs;

public interface Output {
    void out(String line, String lineMetaData);

    void close();
}
