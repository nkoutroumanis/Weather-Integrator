package com.github.nkoutroumanis.outputs;

import com.github.nkoutroumanis.parsers.Record;

public interface Output {
    void out(String line, String lineMetaData);

    void close();
}
