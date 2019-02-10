package com.github.nkoutroumanis.syntheticDataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticDatasetJob {

    private static final String path = "/Users/nicholaskoutroumanis/Desktop/ran/";

    private static final double maxLongitude = 26.6041955909;
    private static final double minLongitude = 20.1500159034;

    private static final double maxLatitude = 41.8269046087;
    private static final double minLatitude = 34.9199876979;

    public static void main(String args[]) throws ParseException, FileNotFoundException {

        final SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final Date maxDate = sd.parse("2018-06-30 23:59:59");
        final Date minDate = sd.parse("2017-06-01 00:00:00");

        double lonDiff = maxLongitude - minLongitude;
        double latDiff = maxLatitude - minLatitude;

        Random r = new Random();
        PrintWriter pw;


        for(int i=0;i<77885;i++){
            pw= new PrintWriter(path+ File.separator+i+".csv");

            for(int j=0;j<3412;j++){
                pw.write((r.nextInt(50000)+1)+";"+ String.format("%.6f", minLongitude + Math.random() * lonDiff) +";"+ String.format("%.6f", minLatitude + Math.random() * latDiff) +";"+ sd.format(new Date(ThreadLocalRandom.current().nextLong(minDate.getTime(), maxDate.getTime())))+"\r\n");
            }
            pw.close();
        }

            pw= new PrintWriter(path+ File.separator+77885+".csv");

            for(int j=0;j<8182;j++){
                pw.write((r.nextInt(50000)+1)+";"+ String.format("%.6f", minLongitude + Math.random() * lonDiff) +";"+ String.format("%.6f", minLatitude + Math.random() * latDiff) +";"+ sd.format(new Date(ThreadLocalRandom.current().nextLong(minDate.getTime(), maxDate.getTime())))+"\r\n");
            }
            pw.close();





    }
}
