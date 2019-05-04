package com.github.nkoutroumanis.kNNSequential;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.Parser;
import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.checkSpatialInfo.CheckSpatialInfo;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class kNNSeq implements FilesParse {

    private final Parser parser;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private String separator;
    private final Rectangle rectangle;

    private List<Map.Entry<Double, String>> list;
    private double maxDistance;

    private Point point;
    private int neighboors;

    public static class Builder {

        private final Parser parser;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;

        private String separator = ";";
        private Rectangle rectangle = Rectangle.newRectangle(-180,-90,180,90);

        public Builder(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {

            this.parser = parser;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder filter(Rectangle rectangle){
            this.rectangle = rectangle;
            return this;
        }

        public kNNSeq build() {
            return new kNNSeq(this);
        }

    }

    private kNNSeq(Builder builder){
        parser = builder.parser;
        numberOfColumnDate = builder.numberOfColumnDate;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        dateFormat = builder.dateFormat;

        separator = builder.separator;
        rectangle = builder.rectangle;

    }

    public static Builder newkNNSeq(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {
        return new kNNSeq.Builder(parser, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }

    public List<Map.Entry<Double, String>> findnearest(Point point, int neighboors) throws IOException {

        this.point = point;
        this.neighboors = neighboors;

        if(Rectangle.longitudeOutOfRange.test(point.getX()) ||  Rectangle.latitudeOutOfRange.test(point.getY())){
            try {
                throw new Exception("Point coordinates are wrong");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        list = new ArrayList<>();

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

                kNNCalculations(line, longitude, latitude);

            }
            catch(ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e){
                continue;
            }
        }

        list.sort((o1, o2) -> Double.compare(o1.getKey(), o2.getKey()));

        return list;
    }

    public void kNNCalculations(String line, double longitude, double latitude) {

        double distance = FilesParse.harvesine(point.getX(), point.getY(), longitude, latitude);

        if(list.size() == neighboors){

            if(Double.compare(maxDistance, distance) == 1){

                int j = -1;
                for (int i = 0; i < list.size(); i++) {
                    if(Double.compare(list.get(i).getKey(), maxDistance) == 0){
                        j = i;
                        break;
                    }
                }

                list.set(j, new AbstractMap.SimpleEntry<>(distance, line));

                //find the max key (distance) of entries from the list
                double d = -1;
                for (Map.Entry<Double, String> entry : list) {
                    if(Double.compare(entry.getKey(), d) == 1){
                        d = entry.getKey();
                    }
                }

                maxDistance = d;

            }

        }
        else{

            list.add(new AbstractMap.SimpleEntry<>(distance, line));

            //find the max key (distance) of entries from the list
            double d = -1;
            for (Map.Entry<Double, String> entry : list) {
                if(Double.compare(entry.getKey(), d) == 1){
                    d = entry.getKey();
                }
            }

            maxDistance = d;

        }
    }
}
