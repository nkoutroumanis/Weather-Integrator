package com.github.nkoutroumanis.statistics;

import com.github.nkoutroumanis.weatherIntegrator.JobUsingIndex;

public final class StatisticsJob {


    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

        Statistics.newStatistics("/Users/nicholaskoutroumanis/Desktop/enriched/",
                7, 8, 3, "yyyy-MM-dd HH:mm:ss", 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39)
                .build().exportStatistics("/Users/nicholaskoutroumanis/Desktop/enriched/");

        Runtime rt = Runtime.getRuntime();
        System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
        System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000 + " sec");

        System.out.println("(Number Of Hits)/(Number Of Records): " + Statistics.numberofRecords);

    }

}
