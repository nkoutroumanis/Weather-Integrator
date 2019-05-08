package com.github.nkoutroumanis.datasources;

import java.io.IOException;
import java.util.function.Predicate;

public interface Datasource {

    String[] nextLine();

    boolean hasNextLine() throws IOException;

    static final Predicate<String> empty = (s1) -> (s1.trim().isEmpty());

}
