package com.github.nkoutroumanis.histogram;

import com.github.nkoutroumanis.FileOutput;
import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.Parser;
import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.dbDataInsertion.MongoDbDataInsertion;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.bson.Document;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public final class GridPartition {

    private final Rectangle rectangle;
    private final long cellsInXAxis;
    private final long cellsInYAxis;
    private final Parser parser;
    private final int numberOfColumnLongitude;
    private final int numberOfColumnLatitude;
    private final int numberOfColumnDate;
    private final DateFormat dateFormat;

    private final String separator;

    private String exportPath;
    private Map<Long, Long> map;
    private double x;
    private double y;

    public static class Builder {

        private Rectangle rectangle;
        private final long cellsInXAxis;
        private final long cellsInYAxis;
        private final Parser parser;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;
        private final DateFormat dateFormat;

        private String separator = ";";

        public Builder(Rectangle rectangle, long cellsInXAxis, long cellsInYAxis, Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {
            this.rectangle = rectangle;
            this.cellsInXAxis = cellsInXAxis;
            this.cellsInYAxis = cellsInYAxis;
            this.parser = parser;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;
            this.dateFormat = new SimpleDateFormat(dateFormat);

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
        rectangle = builder.rectangle;
        cellsInXAxis = builder.cellsInXAxis;
        cellsInYAxis = builder.cellsInYAxis;
        parser = builder.parser;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnDate = builder.numberOfColumnDate;
        dateFormat = builder.dateFormat;

        separator = builder.separator;
    }


    public static Builder newGridPartition(Rectangle rectangle, long cellsInXAxis, long cellsInYAxis, Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {
        return new Builder(rectangle, cellsInXAxis, cellsInYAxis, parser, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }

    public void exportHistogram(String exportPath) throws IOException {

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

        x = (rectangle.getMaxx() - rectangle.getMinx()) / cellsInXAxis;
        y = (rectangle.getMaxy() - rectangle.getMiny()) / cellsInYAxis;


        while (parser.hasNextLine()){

            try {
                String[] a = parser.nextLine();

                String line = a[0];
                String[] separatedLine = line.split(separator);

                if (Parser.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnLatitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnDate - 1])) {
                    continue;
                }

                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);
                Date d = dateFormat.parse(separatedLine[numberOfColumnDate - 1]);

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                    continue;
                }

                insertToHistogram(longitude, latitude);

            }
            catch(ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e){
                continue;
            }
        }


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
            properties.put("rectangle", rectangle);
            properties.put("cellsInXAxis", cellsInXAxis);
            properties.put("cellsInYAxis", cellsInYAxis);
            gson.toJson(properties, writer);

        } catch (IOException e) {
            e.printStackTrace();
        }

        FileOutput fileOutput = FileOutput.newFileOutput(exportPath, false);
        String s = "histogram-info.txt";
        fileOutput.out("Number Of Cells: " + (cellsInXAxis * cellsInYAxis),s);
        fileOutput.out("Number Of Cells in X Axis: " + cellsInXAxis,s);
        fileOutput.out("Number Of Cells in Y Axis: " + cellsInYAxis,s);
        fileOutput.out("Number Of Filled Cells: " + map.size(),s);
        fileOutput.out("Number Of Empty Cells: " + ((cellsInXAxis * cellsInYAxis) - map.size()),s);
        fileOutput.out("Percentage of Filled Cells: " + (((float) map.size()) / ((float) cellsInXAxis * cellsInYAxis)),s);
        fileOutput.out("Percentage of Empty Cells: " + (((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) cellsInXAxis * cellsInYAxis)),s);
        fileOutput.out("Empty Cells/Filled Cells: " + ((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) map.size()),s);
        fileOutput.out("Minimum number contained in a cell: " + Collections.min(map.values()),s);
        fileOutput.out("Maximum number contained in a cell: " + Collections.min(map.values()),s);

        fileOutput.close();
    }

    private void insertToHistogram(double longitude, double latitude) {

        long xc = (long) ((longitude - rectangle.getMinx()) / x);
        long yc = (long) ((latitude - rectangle.getMiny()) / y);

        long k = xc + (yc * cellsInXAxis);

        if (map.containsKey(k)) {
            map.replace(k, map.get(k) + 1);
        } else {
            map.put(k, 1l);
        }
    }

}
