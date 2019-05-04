package com.github.nkoutroumanis;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

public interface Parser {

    String[] nextLine();

    boolean hasNextLine() throws IOException;

    static final Predicate<String> empty = (s1) -> (s1.trim().isEmpty());


}
