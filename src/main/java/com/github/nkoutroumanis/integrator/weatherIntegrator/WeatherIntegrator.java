package com.github.nkoutroumanis.integrator.weatherIntegrator;

import com.github.nkoutroumanis.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class WeatherIntegrator {

    private final Parser parser;
    private final WeatherDataObtainer wdo;

    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private final String separator;

    public static class Builder {

        private final Parser parser;

        private final String gribFilesFolderPath;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;
        private final List<String> variables;

        private String gribFilesExtension = ".grb2";
        private String separator = ";";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;

        public Builder(String filesPath, String filesExtension, String gribFilesFolderPath, int numberOfColumnDate, int numberOfColumnLatitude, int numberOfColumnLongitude, String dateFormat, List<String> variables) throws IOException {

            parser = new FileParser(filesPath, filesExtension);
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
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


        public WeatherIntegrator build() throws IOException {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(WeatherIntegrator.Builder builder) throws IOException {

        parser = builder.parser;

        numberOfColumnDate = builder.numberOfColumnDate;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        dateFormat = builder.dateFormat;

        separator = builder.separator;

        wdo = WeatherDataObtainer.newWeatherDataObtainer(builder.gribFilesFolderPath, builder.gribFilesExtension, builder.lruCacheMaxEntries, builder.useIndex, builder.variables);

    }

    private void clearExportingDirectory(String filesExportPath) {
        //delete existing exported files on the export path
        if (Files.exists(Paths.get(filesExportPath))) {
            Stream.of(new File(filesExportPath).listFiles())/*.filter((file -> file.toString().endsWith(filesExtension)))*/.forEach(File::delete);
        }
    }

    public void integrateAndOutputToDirectory(String directory) throws IOException, ParseException {
        clearExportingDirectory(directory);
        integrate(FileOutput.newFileOutput(directory));
    }

//    public void integrateAndOutputToKafkaTopic(){
//        integrate(Output output)
//    }

    private void integrate(Output output) throws IOException, ParseException {

        while (parser.hasNextLine()){

            String[] a = parser.nextLine();

            String line = a[0];
            String[] separatedLine = line.split(separator);

            if (WeatherIntegrator.empty.test(separatedLine[numberOfColumnLongitude - 1]) || WeatherIntegrator.empty.test(separatedLine[numberOfColumnLatitude - 1]) || WeatherIntegrator.empty.test(separatedLine[numberOfColumnDate - 1])) {
                continue;
            }

            double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
            double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

            StringBuilder sb = new StringBuilder();

            //if dataset finishes with ;
            sb.append(line.substring(0, line.length() - 1));
            //else sb.append(line);

            numberofRecords++;

            List<String> values = wdo.obtainAttributes(longitude, latitude, dateFormat.parse(separatedLine[numberOfColumnDate - 1]));

            values.forEach(s -> sb.append(separator + s));

            output.out(sb + "\r\n", a[1]);

        }

    }

    public static WeatherIntegrator.Builder newWeatherIntegrator(String filesPath, String filesExtension, String gribFilesFolderPath, int numberOfColumnDate, int numberOfColumnLatitude, int numberOfColumnLongitude, String dateFormat, List<String> variables) throws IOException {
        return new WeatherIntegrator.Builder(filesPath, filesExtension, gribFilesFolderPath, numberOfColumnDate, numberOfColumnLatitude, numberOfColumnLongitude, dateFormat, variables);
    }

//    public void startIntegration() throws IOException {
//
//        while (parser.hasNextLine()){
//
//             parser.nextLine();
//
//
//        }
//
//    }






        //private FileOutputStream fos;
    //private OutputStreamWriter osw;
    //private BufferedWriter bw;
    //private PrintWriter pw;

    //public static long hits = 0;
    //public static long numberofRecords = 0;


//    public static void main(String args[]){
//
//
//        WeatherDataObtainer wdo = WeatherDataObtainer.newWeatherDataObtainer("/.../.../", Arrays.asList("")).build();
//
//        try {
//            FileParser fp = new FileParser("/.../...",".csv");
//
//
//
//
//            while (fp.hasNextLine()){
//
//                if()
//                fp.nextLine()
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//
//        wdo.obtainAttributes();
//    }

    public static long hits = 0;
    public static long numberofRecords = 0;

    static final Predicate<String> empty = (s1) -> (s1.trim().isEmpty());

}
