package com.github.nkoutroumanis.kNNSequential;

import com.github.nkoutroumanis.FilesParse;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class kNNSeq implements FilesParse {

    private final String filesPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private String filesExtension;
    private String separator;

    private List<Map.Entry<Double, String>> list;
    private double maxDistance;

    private Point point;
    private int neighboors;

    public static class Builder {

        private final String filesPath;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;

        private String filesExtension = ".csv";
        private String separator = ";";


        public Builder(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {

            this.filesPath = filesPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public kNNSeq build() {
            return new kNNSeq(this);
        }

    }

    private kNNSeq(Builder builder){
        filesPath = builder.filesPath;
        numberOfColumnDate = builder.numberOfColumnDate;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        dateFormat = builder.dateFormat;

        filesExtension = builder.filesExtension;
        separator = builder.separator;

    }

    public static Builder newkNNSeq(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {
        return new kNNSeq.Builder(filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }

    public List<Map.Entry<Double, String>> findnearest(Point point, int neighboors){

        this.point = point;
        this.neighboors = neighboors;

        if(FilesParse.longitudeOutOfRange.test(point.getX()) ||  FilesParse.latitudeOutOfRange.test(point.getY())){
            try {
                throw new Exception("Point coordinates are wrong");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        list = new ArrayList<>();

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);

        list.sort((o1, o2) -> Double.compare(o1.getKey(), o2.getKey()));

        return list;
    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {

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
