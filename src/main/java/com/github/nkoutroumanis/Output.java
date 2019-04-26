package com.github.nkoutroumanis;

public interface Output {
    void out(String line, String lineMeta);

    void close();
}
