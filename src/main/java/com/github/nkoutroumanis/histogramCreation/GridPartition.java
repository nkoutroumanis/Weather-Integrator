package com.github.nkoutroumanis.histogramCreation;

import com.github.nkoutroumanis.FilesParse;

import java.io.*;
import java.util.*;

public class GridPartition implements FilesParse {

    private final Space2D space2D;
    private final int cellsInXAxis;
    private final int cellsInYAxis;
    private final String filesPath;
    private final int numberOfColumnLongitude;
    private final int numberOfColumnLatitude;
    private final int numberOfColumnDate;

    private final String filesExtension;
    private final String separator;

    private String exportPath;
    private Map<Integer,Integer> map;
    private double x;
    private double y;

    public static class Builder{

        private final Space2D space2D;
        private final int cellsInXAxis;
        private final int cellsInYAxis;
        private final String filesPath;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;

        private String filesExtension = ".csv";
        private String separator = ";";

        public Builder(Space2D space2D, int cellsInXAxis, int cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate){
            this.space2D = space2D;
            this.cellsInXAxis = cellsInXAxis;
            this.cellsInYAxis = cellsInYAxis;
            this.filesPath = filesPath;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;

        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public void exportHistogram(String exportPath) {
            new GridPartition(this).exportHistogram(exportPath);
        }
    }

    private GridPartition(Builder builder) {
        space2D = builder.space2D;
        cellsInXAxis = builder.cellsInXAxis;
        cellsInYAxis = builder.cellsInYAxis;
        filesPath = builder.filesPath;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnDate = builder.numberOfColumnDate;

        filesExtension = builder.filesExtension;
        separator = builder.separator;
    }


    public static Builder newGridPartition(Space2D space2D, int cellsInXAxis, int cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate){
        return new Builder( space2D, cellsInXAxis, cellsInYAxis, filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
    }

    private void exportHistogram(String exportPath){
        this.exportPath = exportPath;
        map = new HashMap<>();

        x = ((double) (space2D.getMaxx()-space2D.getMinx())) /cellsInXAxis;
        y = ((double) (space2D.getMaxy()-space2D.getMiny())) /cellsInYAxis;
    //x = 149D / cellsInXAxis;
        //y = 60D / cellsInYAxis;

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);

        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new FileOutputStream("/home/nikolaos/Desktop/histogram.ser"));
            out.writeObject(map);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, float longitude, float  latitude){

        int xc = (int) (Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]) / x);

        int yc = (int) (Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]) / y);

        int k = xc + (yc * cellsInXAxis);

        if(map.containsKey(k)){
            map.replace(k,map.get(k)+1);
        }
        else{
            map.put(k,1);
        }

    }


//    public void createHistogram(String fileExportPath){
//
//        System.out.println(x);
//        System.out.println(y);
//
//
//        //for each file do data integration
//        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {
//
//            stream.forEach((path) -> {
//
//                try (Stream<String> innerStream = Files.lines(path)) {
//
//                    //for each line
//                    innerStream.forEach(line -> {
//
//                                long t1 = System.currentTimeMillis();
//
//                                JobUsingIndex.numberofRows++;
//
//                                String[] separatedLine = line.split(separator);
//
//                        {
//
//                        }
//
//                                if ((separatedLine[numberOfColumnDate - 1].isEmpty() || separatedLine[numberOfColumnLatitude - 1].isEmpty() || separatedLine[numberOfColumnLongitude - 1].isEmpty()) || (separatedLine[numberOfColumnDate - 1].equals("") || separatedLine[numberOfColumnLatitude - 1].equals("") || separatedLine[numberOfColumnLongitude - 1].equals(""))) {
//                                    //pw.write(line + ";;;;;;;;;;;;;" + "\r\n");
////                                    if (!fileswithProblem.contains(path.toString())) {
////                                        fileswithProblem.add(path.toString());
////                                    }
//
//                                } else if ((Float.compare(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), 180) == 1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), -180) == -1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), 90) == 1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), -90) == -1)) {
//
//                                    System.out.println("entopistikan lathos sintetagmenes LONGITUDE:" + Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]) + " LATITUDE:" + Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]) + " " + path.toString());
//
////                                    if (!cordinatesProblem.contains(path.toString())) {
////                                        cordinatesProblem.add(path.toString());
////                                    }
//
//                                } else {
//
//
//                                }
//
//                            }
//                    );
//                } catch (IOException ex) {
//                    Logger.getLogger(JobUsingIndex.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            });
//        } catch (
//                IOException ex) {
//            Logger.getLogger(JobUsingIndex.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }

}
