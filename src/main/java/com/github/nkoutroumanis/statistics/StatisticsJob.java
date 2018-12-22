package com.github.nkoutroumanis.statistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StatisticsJob {


    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

            Statistics.newStatistics("/Users/nicholaskoutroumanis/Desktop/Enriched CSV Files/",
                    7, 8, 3, "yyyy-MM-dd HH:mm:ss",27,28,29,30,31,32,33,34,35,36,37,38,39)
                   .build().exportStatistics("/Users/nicholaskoutroumanis/Desktop/untitled/");

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000 + " sec");


    }

}
