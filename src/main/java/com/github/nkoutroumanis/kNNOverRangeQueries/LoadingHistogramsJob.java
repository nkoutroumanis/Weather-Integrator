package com.github.nkoutroumanis.kNNOverRangeQueries;

public class LoadingHistogramsJob {

    public static void main(String[] args){

        //LoadHistogram lh = LoadHistogram.newLoadHistogram("/Users/nicholaskoutroumanis/Downloads/histograms/1/");
        LoadHistogram lh = LoadHistogram.newLoadHistogram("/Users/nicholaskoutroumanis/Downloads/histograms/1/");

        RadiusDetermination rd = RadiusDetermination.newRadiusDetermination(lh.getHistogram(), lh.getNumberOfCellsxAxis(), lh.getNumberOfCellsyAxis(), lh.getMinx(), lh.getMiny(), lh.getMaxx(), lh.getMaxy());

        rd.findRadius(23.709028,37.957193,1);

    }
}
