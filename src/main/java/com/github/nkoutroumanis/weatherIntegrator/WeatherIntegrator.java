package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public final class WeatherIntegrator {

    private final Parser parser;
    private final WeatherDataObtainer wdo;

    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...

    private final DateFormat dateFormat;

    private final String separator;

    private final Rectangle rectangle;

    public static class Builder {

        private final Parser parser;

        private final String gribFilesFolderPath;

        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...

        private final DateFormat dateFormat;
        private final List<String> variables;

        private String gribFilesExtension = ".grb2";
        private String separator = ";";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;

        private Rectangle rectangle = Rectangle.newRectangle(-180,-90,180,90);

        public Builder(Parser parser, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws Exception {

            this.parser = parser;
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.variables = variables;
        }

        public Builder gribFilesExtension(String gribFilesExtension) {
            this.gribFilesExtension = gribFilesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder lruCacheMaxEntries(int lruCacheMaxEntries) {
            this.lruCacheMaxEntries = lruCacheMaxEntries;
            return this;
        }

        public Builder useIndex() {
            this.useIndex = true;
            return this;
        }

        public Builder filter(Rectangle rectangle){
            this.rectangle = rectangle;
            return this;
        }

        public WeatherIntegrator build() throws IOException {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(WeatherIntegrator.Builder builder) throws IOException {

        parser = builder.parser;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnDate = builder.numberOfColumnDate;
        dateFormat = builder.dateFormat;
        separator = builder.separator;

        wdo = WeatherDataObtainer.newWeatherDataObtainer(builder.gribFilesFolderPath, builder.gribFilesExtension, builder.lruCacheMaxEntries, builder.useIndex, builder.variables);
        rectangle = builder.rectangle;
    }

//    private void clearExportingDirectory(String filesExportPath) throws IOException {
//
//        if(!filesExportPath.substring(filesExportPath.length()-1).equals(File.separator)){
//            filesExportPath = filesExportPath + File.separator;
//        }
//
//        //delete existing exported files on the export path
//        if (Files.exists(Paths.get(filesExportPath))) {
//
//            Stream.of(new File(filesExportPath).listFiles()).forEach(File::delete);
//
//
//            //System.out.println(dirPath.toFile().delete());
//
//        }
//    }

    public void integrateAndOutputToKafkaTopic(KafkaOutput kafkaOutput) throws IOException, ParseException {
        integrate(kafkaOutput);
    }

    public void integrateAndOutputToDirectory(FileOutput fileOutput) throws IOException, ParseException {
        integrate(fileOutput);
    }

    private void integrate(Output output) throws IOException, ParseException {

        start = System.currentTimeMillis();

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


                StringBuilder sb = new StringBuilder();

                //if dataset finishes with ;
                sb.append(line.substring(0, line.length() - 1));
                //else sb.append(line);

                numberofRecords++;

                List<String> values = wdo.obtainAttributes(longitude, latitude, d);

                values.forEach(s -> sb.append(separator + s));

                output.out(sb.toString(), a[1]);
            }
            catch(ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e){
                continue;
            }

        }

        elapsedTime = (System.currentTimeMillis() - start) / 1000;

        output.close();

    }

    public static WeatherIntegrator.Builder newWeatherIntegrator(Parser parser, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws Exception {
        return new WeatherIntegrator.Builder(parser, gribFilesFolderPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate,  dateFormat, variables);
    }

    public static long start;
    public static long elapsedTime;

    public static long hits = 0;
    public static long numberofRecords = 0;

}
