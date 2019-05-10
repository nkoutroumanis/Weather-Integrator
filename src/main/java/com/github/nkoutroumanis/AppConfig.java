package com.github.nkoutroumanis;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {
    public static final Config config = ConfigFactory.load();
}
