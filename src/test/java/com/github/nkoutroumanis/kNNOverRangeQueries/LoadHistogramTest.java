package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class LoadHistogramTest {

    @Test
    public void newReadHistogram() {
        Config conf = ConfigFactory.parseFile(new File("/Users/nicholaskoutroumanis/Downloads/histograms/5/properties.json"));
        System.out.println(conf.getDouble("space2D.maxx"));

    }
}