package com.github.nkoutroumanis.histogram;

import com.github.nkoutroumanis.FilesParse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class GridPartition implements FilesParse {

    private final Space2D space2D;
    private final long cellsInXAxis;
    private final long cellsInYAxis;
    private final String filesPath;
    private final int numberOfColumnLongitude;
    private final int numberOfColumnLatitude;
    private final int numberOfColumnDate;

    private final String filesExtension;
    private final String separator;

    private String exportPath;
    private Map<Long, Long> map;
    private double x;
    private double y;

    public static class Builder {

        private final Space2D space2D;
        private final long cellsInXAxis;
        private final long cellsInYAxis;
        private final String filesPath;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;

        private String filesExtension = ".csv";
        private String separator = ";";

        public Builder(Space2D space2D, long cellsInXAxis, long cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {
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

        public GridPartition build() {
            return new GridPartition(this);
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


    public static Builder newGridPartition(Space2D space2D, long cellsInXAxis, long cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {
        return new Builder(space2D, cellsInXAxis, cellsInYAxis, filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
    }

    public void exportHistogram(String exportPath) {

        Path path = Paths.get(exportPath);
        //if directory exists does not exist
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                //fail to create directory
                e.printStackTrace();
            }
        }

        this.exportPath = exportPath;
        this.map = new HashMap<>();

        x = (space2D.getMaxx() - space2D.getMinx()) / cellsInXAxis;
        y = (space2D.getMaxy() - space2D.getMiny()) / cellsInYAxis;

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);

        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new FileOutputStream(exportPath + File.separator + "histogram.ser"));
            out.writeObject(map);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (Writer writer = new FileWriter(exportPath + File.separator + "properties.json")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            Map<String, Object> properties = new HashMap<>();
            properties.put("space2D", space2D);
            properties.put("cellsInXAxis", cellsInXAxis);
            properties.put("cellsInYAxis", cellsInYAxis);
            gson.toJson(properties, writer);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Number Of Cells: " + (cellsInXAxis * cellsInYAxis));
        System.out.println("Number Of Cells in X Axis: " + cellsInXAxis);
        System.out.println("Number Of Cells in Y Axis: " + cellsInYAxis);
        System.out.println("Number Of Filled Cells: " + map.size());
        System.out.println("Number Of Empty Cells: " + ((cellsInXAxis * cellsInYAxis) - map.size()));
        System.out.println("Percentage of Filled Cells: " + (((float) map.size()) / ((float) cellsInXAxis * cellsInYAxis)));
        System.out.println("Percentage of Empty Cells: " + (((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) cellsInXAxis * cellsInYAxis)));
        System.out.println("Empty Cells/Filled Cells: " + ((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) map.size()));


        System.out.println(Collections.min(map.values()));
        System.out.println(Collections.max(map.values()));

    }


    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {

        long xc = (long) (longitude / x);
        long yc = (long) (latitude / y);

        long k = xc + (yc * cellsInXAxis);

        if (map.containsKey(k)) {
            map.replace(k, map.get(k) + 1);
        } else {
            map.put(k, 1l);
        }

//        if(Double.compare(longitude,26.604197)==1 || Double.compare(longitude,20.150010)==-1){
//            System.out.println("Out of margins");
//        }
//
//        if(Double.compare(latitude, 41.826906)==1 || Double.compare(latitude,34.919987)==-1){
//            System.out.println("Out of margins");
//        }


        long xc1 = (long) ((longitude - space2D.getMinx()) / x);

        if(Long.compare(xc1,xc-6l)==0){

            System.out.println("its OK "+ "xc "+ xc+ " xc1 "+ xc1);
        }
        else{

            System.out.println("long "+ longitude + " x "  + x + " xc "+xc + " xc-624="+(xc-6l));
            System.out.println("long "+ longitude + " x "  + x + " xc1 "+xc1);


            System.out.println("its not ok");
        }

//        if(k1==0){
//            if(k1==(k-202824)){
//                System.out.println("the zero is ok");
//            }
//            else{
//                System.out.println("the zero is not ok");
//                System.out.println(k);
//            }
//        }

//        if(k>(202824l+39999l)){
//            System.out.println(k);
//        }

    }

}
