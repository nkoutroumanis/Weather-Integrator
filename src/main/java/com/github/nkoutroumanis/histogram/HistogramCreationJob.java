package com.github.nkoutroumanis.histogram;

public final class HistogramCreationJob {
    public static void main(String args[]) {
        //x for longitude, y for latitude- the max values of lon and lat should be increased a little in order to include the whole data in histogram
        Space2D space = Space2D.newSpace2D(-26.64, 0, 121.57, 59.94);
//        long t1;
//        int j = 1;
//        for(long i = 100000; i<=100000*10;i = i + 100000){
//            t1 = System.currentTimeMillis();
//            GridPartition.newGridPartition(space, i, i, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/histograms-new/"+j);
//            System.out.println(j + " folder " + ((System.currentTimeMillis()-t1)/1000) + "sec");
//            j++;
//            System.out.println("------------------"); }
//    }

        long t1;
        int j = 1;
            t1 = System.currentTimeMillis();
            GridPartition.newGridPartition(space, 4000000, 4000000, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/large/"+j);
            System.out.println(j + " folder " + ((System.currentTimeMillis()-t1)/1000) + "sec");
            j++;
    }

}
