package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.io.File;

public class LoadHistogramTest {

    @Test
    public void newReadHistogram() {
        Config conf = ConfigFactory.parseFile(new File("./src/test/resources/histogram/properties.json"));
        System.out.println(conf.getDouble("space2D.maxx"));

    }
}