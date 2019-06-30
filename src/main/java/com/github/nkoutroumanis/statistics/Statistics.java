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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public final class Statistics {

    private static final Logger logger = LoggerFactory.getLogger(Statistics.class);

    private final Rectangle rectangle;

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

        long start = System.currentTimeMillis();

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

        fileOutput.out("","Statistics.txt");
        fileOutput.out(n + " records have been parsed","Statistics.txt");

        logger.info("The calculation of the statistical elements has finished!");
        logger.info("{} records have been parsed", n);
        logger.info("See file {}", fileOutput.getDirectory() + "Statistics.txt");
        logger.info("Elapsed time {}", (System.currentTimeMillis() - start) / 1000 + " sec");

        fileOutput.close();

    }

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

    public static Statistics.Builder newStatistics(){
        return new Statistics.Builder();
    }

}
