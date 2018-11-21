package com.github.nkoutroumanis.histogram;

public final class HistogramCreationJob {
    public static void main(String args[]) {

        //x for longitude, y for latitude
        Space2D space = Space2D.newSpace2D(-26.7f, 0f, 122.56f, 60.93f);
        GridPartition.newGridPartition(space, 10000, 10000, "/home/nikolaos/Documents/tambak", 7, 8, 3).build().exportHistogram("/home/nikolaos/Desktop/");
    }

}
