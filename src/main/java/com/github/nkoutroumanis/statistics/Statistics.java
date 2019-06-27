package com.github.nkoutroumanis.statistics;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.JsonRecordParser;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public final class Statistics {

    private static final Logger logger = LoggerFactory.getLogger(Statistics.class);

    private final Rectangle rectangle;
//    private final RecordParser recordParser;
//    private final List<T> t;

//    public static long numberofRecords = 0;
//    private final String filesPath;
//    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
//    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
//    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
//    private final DateFormat dateFormat;
//    private final int[] columns;
//    private final String filesExtension;
//    private final String separator;
//    private final List<Double> maxOfColumnsPerFile;
//    private final List<Double> minOfColumnsPerFile;
//    private final List<Double> sumOfColumnsPerFile;
//    private final List<Double> squaredSumOfColumnsPerFile;
//    private final List<Double> stdOfColumnsPerFile;



//    private String filesExportPath;
//    private List<List<Double>> columnsInFile;
//    private FileOutputStream fos;
//    private OutputStreamWriter osw;
//    private BufferedWriter bw;
//    private PrintWriter pw;

    private Statistics(Builder builder) {
        rectangle = builder.rectangle;
    }

    public void calculateElementsFromCSVformat(CsvRecordParser csvRecordParser, List<Integer> attributesIndex, FileOutput fileOutput) throws IOException, ParseException {
        calculateElements(csvRecordParser, attributesIndex, fileOutput, (o1, record)->{

            int index = o1-1;
            return Double.parseDouble((String) record.getFieldValues().get(index));

            });

    }

    public void calculateElementsFromJsonformat(JsonRecordParser jsonRecordParser, List<String> attributesNames, FileOutput fileOutput) throws IOException, ParseException {
        calculateElements(jsonRecordParser, attributesNames, fileOutput, (fieldName,record)->{

            int k=0;

            for(int i=0;i<record.getFieldNames().size();i++){
                if(record.getFieldNames().get(k).equals(fieldName)){
                    break;
                }
                k++;
            }

            if(k==record.getFieldNames().size()){
                try {
                    throw new Exception("fieldName does not exist");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            return Double.parseDouble(((ConfigValue) record.getFieldValues().get(k)).unwrapped().toString());

            });
    }

    public <T> void calculateElements(RecordParser recordParser, List<T> attributes, FileOutput fileOutput, BiFunction<T, Record, Double> bifunction) throws IOException {

        long n = 0;

        List<Double> maxOfColumns = new ArrayList<>();
        List<Double> minOfColumns = new ArrayList<>();
        List<Double> sumOfColumns = new ArrayList<>();
        List<Double> squaredSumOfColumns = new ArrayList<>();

        attributes.forEach(i->{
            maxOfColumns.add((double) Long.MIN_VALUE);
            minOfColumns.add((double) Long.MAX_VALUE);
            sumOfColumns.add(0d);
            squaredSumOfColumns.add(0d);
        });

        while (recordParser.hasNextRecord()) {

            try {
                Record record = recordParser.nextRecord();
                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));

                if (rectangle != null) {
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        logger.warn("Spatial information of record out of range \nLine{}", record.getMetadata());
                        continue;
                    }
                }

                for(int i = 0;i<attributes.size();i++)
                {

                    double value = bifunction.apply(attributes.get(i), record);

                    if(Double.compare(value, maxOfColumns.get(i)) == 1){
                        maxOfColumns.set(i,value);
                    }

                    if(Double.compare(value, minOfColumns.get(i)) == -1){
                        minOfColumns.set(i,value);
                    }

                    sumOfColumns.set(i, sumOfColumns.get(i) + value);
                    squaredSumOfColumns.set(i, Math.pow(value, 2) + squaredSumOfColumns.get(i));

                 }

                n++;

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        for(int i = 0;i<attributes.size();i++){
            StringBuilder sb = new StringBuilder();
            sb.append("Column " + attributes.get(i) + " - ");
            sb.append("Max: " + maxOfColumns.get(i) +" ");
            sb.append("Min: " + minOfColumns.get(i) +" ");
            sb.append("Average: "+ sumOfColumns.get(i)/n +" ");
            sb.append("Std: "+ ((squaredSumOfColumns.get(i)/n) - Math.pow(sumOfColumns.get(i)/n, 2)));
            fileOutput.out(sb.toString(),"Statistics.txt");
        }

        fileOutput.close();

    }

//    public static Builder newStatistics(CsvRecordParser csvRecordParser, List<Integer> columns) {
//        return new Statistics.Builder(csvRecordParser, columns);
//    }
//
//    public static Builder newStatistics(JsonRecordParser jsonRecordParser, List<String> fields) {
//        return new Statistics.Builder(jsonRecordParser, fields);
//    }

//    private void clearExportingDirectory() {
//        //delete existing exported files on the export path
//        if (Files.exists(Paths.get(filesExportPath))) {
//            Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
//        }
//    }
//
//    @Override
//    public void fileParse(Path filePath) {
//
//        columnsInFile = new ArrayList<>();
//
//        for (int i : columns) {
//            columnsInFile.add(new ArrayList<>());
//        }
//    }
//
//    @Override
//    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
//        Statistics.numberofRecords++;
//        for (int i = 0; i < columns.length; i++) {
//            columnsInFile.get(i).add(Double.parseDouble(separatedLine[columns[i] - 1]));
//        }
//
//    }
//
//
//    @Override
//    public void afterLineParse() {
//
//        for (int i = 0; i < columns.length; i++) {
//            DoubleSummaryStatistics dss = columnsInFile.get(i).stream().mapToDouble(Double::doubleValue).summaryStatistics();
//
//            sumOfColumnsPerFile.set(i, sumOfColumnsPerFile.get(i) + dss.getSum());
//
//            if (Double.compare(dss.getMax(), maxOfColumnsPerFile.get(i)) == 1) {
//                maxOfColumnsPerFile.set(i, dss.getMax());
//            }
//
//            if (Double.compare(dss.getMin(), minOfColumnsPerFile.get(i)) == -1) {
//                minOfColumnsPerFile.set(i, dss.getMin());
//            }
//
//
//            double sum = 0;
//            for (Double e : columnsInFile.get(i)) {
//                sum = sum + Math.pow(e - dss.getAverage(), 2);
//            }
//
//            double ressStd = Math.sqrt(sum / (columnsInFile.get(i).size() - 1));
//
//            if (stdOfColumnsPerFile.size() == columns.length) {
//                stdOfColumnsPerFile.set(i, (stdOfColumnsPerFile.get(i) + ressStd) / 2);
//            } else {//list initialization
//                stdOfColumnsPerFile.add(ressStd);
//            }
//        }
//
//    }
//
//    public void exportStatistics(String filesExportPath) {
//
//        this.filesExportPath = filesExportPath;
//
//        clearExportingDirectory();
//
//        //create Export Directory
//        try {
//            Files.createDirectories(Paths.get(filesExportPath));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
//
//
//        try (FileOutputStream fos = new FileOutputStream(filesExportPath + File.separator + "Statistics.txt", true);
//             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)) {
//
//            for (int i = 0; i < columns.length; i++) {
//
//                pw.write("Column Number " + columns[i] + ": Average: " + (sumOfColumnsPerFile.get(i) / numberofRecords) + ", ");
//                pw.write("Max: " + maxOfColumnsPerFile.get(i) + ", ");
//                pw.write("Min: " + minOfColumnsPerFile.get(i) + ", ");
//                pw.write("Std: " + stdOfColumnsPerFile.get(i) + "\r\n");
//            }
//
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

    public static class Builder {

        private Rectangle rectangle;

        public Builder() {

        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public Statistics build() {
            return new Statistics(this);
        }

    }


}
