package com.github.nkoutroumanis.kNNSequential;

import java.util.List;
import java.util.Map;

public class ExampleJob {

    public static void main(String args[]){

       List<Map.Entry<Double, String>> l = kNNSeq.newkNNSeq("/Users/nicholaskoutroumanis/Desktop/csv/", 7,
                8, 3, "yyyy-MM-dd HH:mm:ss").build().findnearest(Point.newPoint(25.167908,35.322244),10);

       l.forEach(i -> System.out.println(i.getKey()));
    }
}
