package com.github.nkoutroumanis.histogram;

public final class HistogramCreationJob {
    public static void main(String args[]) {
//        //x for longitude, y for latitude - the max values of lon and lat should be increased a little in order to include the whole data in histogram
//        Space2D space = Space2D.newSpace2D(-26.64, 0, 121.57, 59.94);
//        //Space2D space = Space2D.newSpace2D(20.1500159034, 34.9199876979, 26.61, 41.83);
//        long t1;
//        int j = 1;
//        for (long i = 1000; i <= 10000; i = i + 1000) {
//            t1 = System.currentTimeMillis();
//
//            GridPartition.newGridPartition(space, i, i, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/greek-hist/" + j);
//
//            System.out.println(j + " folder " + ((System.currentTimeMillis() - t1) / 1000) + "sec");
//            j++;
//            System.out.println("------------------");
//
//        }
//    }

//        long t1;
//        int j = 1;
//            t1 = System.currentTimeMillis();
//            GridPartition.newGridPartition(space, 4000000, 4000000, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/large/"+j);
//            System.out.println(j + " folder " + ((System.currentTimeMillis()-t1)/1000) + "sec");
//            j++;


        //x for longitude, y for latitude - the max values of lon and lat should be increased a little in order to include the whole data in histogram
        Space2D space = Space2D.newSpace2D(20.15, 34.91, 26.61, 41.83);
        //Space2D space = Space2D.newSpace2D(20.1500159034, 34.9199876979, 26.604196, 41.826905);
        //Space2D space = Space2D.newSpace2D(20, 34.9199876979, 27, 41.826905);
        GridPartition.newGridPartition(space, 2, 2, "/home/nikolaos/Documents/synthetic-dataset2", 2, 3, 4).build().exportHistogram("/home/nikolaos/Documents/hist/synthetic-dataset2/");
        System.out.println("------------------");

    }
}