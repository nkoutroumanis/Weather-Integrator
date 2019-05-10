package com.github.nkoutroumanis.outputs;

import com.github.nkoutroumanis.parsers.Record;

public interface Output {
    void out(Record record);

    void close();
}
