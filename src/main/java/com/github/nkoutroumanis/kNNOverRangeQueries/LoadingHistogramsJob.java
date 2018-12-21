package com.github.nkoutroumanis.kNNOverRangeQueries;

public class LoadingHistogramsJob {

    public static void main(String[] args){

        //LoadHistogram lh = LoadHistogram.newLoadHistogram("/Users/nicholaskoutroumanis/Downloads/histograms/1/");
        LoadHistogram lh = LoadHistogram.newLoadHistogram("/home/nikolaos/Documents/histograms-new/10/");

        RadiusDetermination rd = RadiusDetermination.newRadiusDetermination(lh.getHistogram(), lh.getNumberOfCellsxAxis(), lh.getNumberOfCellsyAxis(), lh.getMinx(), lh.getMiny(), lh.getMaxx(), lh.getMaxy());

        System.out.println(lh.getMaxx());
        System.out.println(lh.getMinx());
        System.out.println(lh.getMaxy());
        System.out.println(lh.getMiny());

        rd.findRadius(57.19,2.81,1);

    }
}
