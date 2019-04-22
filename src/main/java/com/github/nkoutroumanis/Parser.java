package com.github.nkoutroumanis;

import java.io.IOException;
import java.util.Map;

public interface Parser {

    Map.Entry<String, String> nextLine();

    boolean hasNextLine() throws IOException;

}
