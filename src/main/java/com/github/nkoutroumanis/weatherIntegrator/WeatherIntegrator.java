package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

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

        private Rectangle rectangle = null;

        public Builder(String filesPath, String filesExtension, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws IOException {

            parser = FileParser.newFileParser(filesPath, filesExtension);
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.variables = variables;
        }

        public Builder(String propertiesFile, String topicName, long poll, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws IOException {

            parser = KafkaParser.newKafkaParser(propertiesFile, topicName, poll);
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

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public void integrateAndOutputToKafkaTopic(String properties, String topicName) throws IOException, ParseException {
        integrate(KafkaOutput.newKafkaOutput(properties, topicName));
    }

    public void integrateAndOutputToDirectory(String directory) throws IOException, ParseException {

        if(!directory.substring(directory.length()-1).equals(File.separator)){
            directory = directory + File.separator;
        }

        deleteDirectory(new File(directory));
        integrate(FileOutput.newFileOutput(directory));
    }

//    public void integrateAndOutputToKafkaTopic(){
//        integrate(KafkaOutput.newKafkaOutput());
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

            //filtering
            if(rectangle != null){
                if(((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))){
                    continue;
                }
            }

            StringBuilder sb = new StringBuilder();

            //if dataset finishes with ;
            sb.append(line.substring(0, line.length() - 1));
            //else sb.append(line);

            numberofRecords++;

            List<String> values = wdo.obtainAttributes(longitude, latitude, dateFormat.parse(separatedLine[numberOfColumnDate - 1]));

            values.forEach(s -> sb.append(separator + s));

            output.out(sb.toString() , a[1]);

        }

            output.close();

    }

    public static WeatherIntegrator.Builder newWeatherIntegrator(String filesPath, String filesExtension, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws IOException {
        return new WeatherIntegrator.Builder(filesPath, filesExtension, gribFilesFolderPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate,  dateFormat, variables);
    }

    public static WeatherIntegrator.Builder newWeatherIntegrator(String propertiesFile, String topicName, long poll, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) throws IOException {
        return new WeatherIntegrator.Builder(propertiesFile, topicName, poll, gribFilesFolderPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate,  dateFormat, variables);
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
