package com.github.nkoutroumanis;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {
    private static AppConfig instance = null;

    private final Config config;

    private AppConfig() {
        config = ConfigFactory.load();
    }

    public Config getConfig() {
        return config;
    }

    public static AppConfig getInstance() {
        if (instance == null) {
            instance = new AppConfig();
        }
        return instance;
    }
}
