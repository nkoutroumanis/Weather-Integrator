package com.github.nkoutroumanis.histogram;

import com.github.nkoutroumanis.FilesParse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class GridPartition implements FilesParse {

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
    private Map<Integer, Integer> map;
    private double x;
    private double y;

    public static class Builder {

        private final Space2D space2D;
        private final int cellsInXAxis;
        private final int cellsInYAxis;
        private final String filesPath;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;

        private String filesExtension = ".csv";
        private String separator = ";";

        public Builder(Space2D space2D, int cellsInXAxis, int cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {
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


    public static Builder newGridPartition(Space2D space2D, int cellsInXAxis, int cellsInYAxis, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {
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

            Map<String,Object> properties = new HashMap<>();
            properties.put("space2D",space2D);
            properties.put("cellsInXAxis", cellsInXAxis);
            properties.put("cellsInYAxis", cellsInYAxis);
            gson.toJson(properties, writer);

        } catch (IOException e) {
            e.printStackTrace();
        }


        PrintWriter writer = null;
        try {
            writer = new PrintWriter(exportPath + File.separator + "hist-desc.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        writer.println(cellsInXAxis + " " + cellsInYAxis);
        writer.close();

        int sum = 0;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            sum += entry.getValue();
        }
        System.out.println(sum);
    }


    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {

        int xc = (int) (longitude / x);

        int yc = (int) (latitude / y);

        int k = xc + (yc * cellsInXAxis);

        if (map.containsKey(k)) {
            map.replace(k, map.get(k) + 1);
        } else {
            map.put(k, 1);
        }

        System.out.println("Number Of Cells: " + (cellsInXAxis * cellsInYAxis));
        System.out.println("Number Of Filled Cells: " + map.size());
        System.out.println("Number Of Empty Cells: " + ((cellsInXAxis * cellsInYAxis) - map.size()));
        System.out.println("Percentage of Filled Cells: " + (((double) map.size()) / ((double) cellsInXAxis * cellsInYAxis)));
        System.out.println("Percentage of Empty Cells: " + ( ((double) map.size()) / ((double) cellsInXAxis * cellsInYAxis)));
        System.out.println("Empty Cells/Filled Cells: " + ((double) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((double) map.size()) );


    }

}
