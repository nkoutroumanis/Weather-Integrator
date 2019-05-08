package com.github.nkoutroumanis.statistics;

import com.github.nkoutroumanis.FilesParse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.stream.Stream;

public final class Statistics implements FilesParse {

    private final String filesPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private final int[] columns;
    private final String filesExtension;
    private final String separator;

    private String filesExportPath;

    private final List<Double> maxOfColumnsPerFile;
    private final List<Double> minOfColumnsPerFile;
    private final List<Double> sumOfColumnsPerFile;
    private final List<Double> stdOfColumnsPerFile;
    private List<List<Double>> columnsInFile;


    private FileOutputStream fos;
    private OutputStreamWriter osw;
    private BufferedWriter bw;
    private PrintWriter pw;

    public static long numberofRecords = 0;

    public static class Builder {

        private final String filesPath;

        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;

        private final int[] columns;
        private String filesExtension = ".csv";
        private String separator = ";";

        public Builder(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, int... columns) {
            this.filesPath = filesPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.columns = columns;
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Statistics build() {
            return new Statistics(this);
        }

    }

    private Statistics(Builder builder) {
        filesPath = builder.filesPath;

        numberOfColumnDate = builder.numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        dateFormat = builder.dateFormat;
        columns = builder.columns;
        filesExtension = builder.filesExtension;
        separator = builder.separator;

        maxOfColumnsPerFile = new ArrayList<>();
        for (int i : columns) {
            maxOfColumnsPerFile.add((double) Long.MIN_VALUE);
        }

        minOfColumnsPerFile = new ArrayList<>();
        for (int i : columns) {
            minOfColumnsPerFile.add((double) Long.MAX_VALUE);
        }

        sumOfColumnsPerFile = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            sumOfColumnsPerFile.add(0d);
        }

        stdOfColumnsPerFile = new ArrayList<>();
//        for (int i = 0; i < columns.length; i++) {
//            stdOfColumnsPerFile.add(0d);
//        }
    }

    private void clearExportingDirectory() {
        //delete existing exported files on the export path
        if (Files.exists(Paths.get(filesExportPath))) {
            Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
        }
    }

    @Override
    public void fileParse(Path filePath) {

        columnsInFile = new ArrayList<>();

        for (int i : columns) {
            columnsInFile.add(new ArrayList<>());
        }
    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
        Statistics.numberofRecords++;
        for (int i = 0; i < columns.length; i++) {
            columnsInFile.get(i).add(Double.parseDouble(separatedLine[columns[i] - 1]));
        }

    }


    @Override
    public void afterLineParse() {

        for (int i = 0; i < columns.length; i++) {
            DoubleSummaryStatistics dss = columnsInFile.get(i).stream().mapToDouble(Double::doubleValue).summaryStatistics();

            sumOfColumnsPerFile.set(i, sumOfColumnsPerFile.get(i) + dss.getSum());

            if (Double.compare(dss.getMax(), maxOfColumnsPerFile.get(i)) == 1) {
                maxOfColumnsPerFile.set(i, dss.getMax());
            }

            if (Double.compare(dss.getMin(), minOfColumnsPerFile.get(i)) == -1) {
                minOfColumnsPerFile.set(i, dss.getMin());
            }


            double sum = 0;
            for (Double e : columnsInFile.get(i)) {
                sum = sum + Math.pow(e - dss.getAverage(), 2);
            }

            double ressStd = Math.sqrt(sum / (columnsInFile.get(i).size() - 1));

            if (stdOfColumnsPerFile.size() == columns.length) {
                stdOfColumnsPerFile.set(i, (stdOfColumnsPerFile.get(i) + ressStd) / 2);
            } else {//list initialization
                stdOfColumnsPerFile.add(ressStd);
            }
        }

    }

    public void exportStatistics(String filesExportPath) {

        this.filesExportPath = filesExportPath;

        clearExportingDirectory();

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(filesExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);


        try (FileOutputStream fos = new FileOutputStream(filesExportPath + File.separator + "Statistics.txt", true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)) {

            for (int i = 0; i < columns.length; i++) {

                pw.write("Column Number " + columns[i] + ": Average: " + (sumOfColumnsPerFile.get(i) / numberofRecords) + ", ");
                pw.write("Max: " + maxOfColumnsPerFile.get(i) + ", ");
                pw.write("Min: " + minOfColumnsPerFile.get(i) + ", ");
                pw.write("Std: " + stdOfColumnsPerFile.get(i) + "\r\n");
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Builder newStatistics(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, int... columns) {
        return new Statistics.Builder(filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat, columns);
    }


}
