package com.github.nkoutroumanis.histogram;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class GridPartition {

    private static final Logger logger = LoggerFactory.getLogger(GridPartition.class);


    private final Rectangle rectangle;
    private final long cellsInXAxis;
    private final long cellsInYAxis;
    private final RecordParser recordParser;

    private String exportPath;
    private Map<Long, Long> map;
    private double x;
    private double y;

    private GridPartition(Builder builder) {
        recordParser = builder.recordParser;
        rectangle = builder.rectangle;
        cellsInXAxis = builder.cellsInXAxis;
        cellsInYAxis = builder.cellsInYAxis;
    }

    public static Builder newGridPartition(RecordParser recordParser, Rectangle rectangle, long cellsInXAxis, long cellsInYAxis) throws Exception {
        return new Builder(recordParser, rectangle, cellsInXAxis, cellsInYAxis);
    }

    public void exportHistogram(String exportPath) throws IOException, ParseException {

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

        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());


        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                    logger.warn("Spatial information of record out of range \nLine{}", record.getMetadata());
                    continue;
                }

                insertToHistogram(longitude, latitude);

            } catch (NumberFormatException | ParseException e) {
                logger.warn("Spatio-temporal information of record can not be parsed {} \nLine {}", e, record.getMetadata());
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
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
        fileOutput.out("Number Of Cells: " + (cellsInXAxis * cellsInYAxis), s);
        fileOutput.out("Number Of Cells in X Axis: " + cellsInXAxis, s);
        fileOutput.out("Number Of Cells in Y Axis: " + cellsInYAxis, s);
        fileOutput.out("Number Of Filled Cells: " + map.size(), s);
        fileOutput.out("Number Of Empty Cells: " + ((cellsInXAxis * cellsInYAxis) - map.size()), s);
        fileOutput.out("Percentage of Filled Cells: " + (((float) map.size()) / ((float) cellsInXAxis * cellsInYAxis)), s);
        fileOutput.out("Percentage of Empty Cells: " + (((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) cellsInXAxis * cellsInYAxis)), s);
        fileOutput.out("Empty Cells/Filled Cells: " + ((float) ((cellsInXAxis * cellsInYAxis) - map.size())) / ((float) map.size()), s);
        fileOutput.out("Minimum number contained in a cell: " + Collections.min(map.values()), s);
        fileOutput.out("Maximum number contained in a cell: " + Collections.min(map.values()), s);

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

    public static class Builder {

        private final long cellsInXAxis;
        private final long cellsInYAxis;
        private final RecordParser recordParser;
        private Rectangle rectangle;

        public Builder(RecordParser recordParser, Rectangle rectangle, long cellsInXAxis, long cellsInYAxis) throws Exception {
            this.rectangle = rectangle;
            this.cellsInXAxis = cellsInXAxis;
            this.cellsInYAxis = cellsInYAxis;
            this.recordParser = recordParser;
        }

        public GridPartition build() {
            return new GridPartition(this);
        }
    }

}
