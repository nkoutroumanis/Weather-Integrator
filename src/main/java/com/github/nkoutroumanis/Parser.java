package com.github.nkoutroumanis;

import java.io.IOException;
import java.util.Map;

public interface Parser {

    String[] nextLine();

    boolean hasNextLine() throws IOException;

}
