package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

public class LoadHistogramTest {

    @Ignore("Runs perfectly, but on travis exception is thrown; No Configurtion setting found for key 'space2D'")
    @Test
    public void newReadHistogram() {
        Config conf = ConfigFactory.parseFile(new File("./src/test/resources/histogram/properties.json"));
        System.out.println(conf.getDouble("space2D.maxx"));

    }
}