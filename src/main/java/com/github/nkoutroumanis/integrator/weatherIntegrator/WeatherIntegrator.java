package com.github.nkoutroumanis.integrator.weatherIntegrator;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.integrator.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.integrator.weatherIntegrator.lru.LRUCacheManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Stream;

public final class WeatherIntegrator implements FilesParse {

    //private final String filesPath;

    private final String gribFilesFolderPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    //private final String filesExtension;
    private final String gribFilesExtension;
    private final String separator;
    private final int lruCacheMaxEntries;
    private final boolean useIndex;

    private final LRUCacheManager lruCacheManager;

    //private String filesExportPath;
    //private FileOutputStream fos;
    //private OutputStreamWriter osw;
    //private BufferedWriter bw;
    //private PrintWriter pw;

    //public static long hits = 0;
    //public static long numberofRecords = 0;

    public static class Builder {

        //private final String filesPath;
        private final String gribFilesFolderPath;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;
        private final List<String> variables;

        //private String filesExtension = ".csv";
        private String gribFilesExtension = ".grb2";
        private String separator = ";";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;


        public Builder(String filesPath, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) {

            //this.filesPath = filesPath;
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.variables = variables;
        }

//        public Builder filesExtension(String filesExtension) {
//            this.filesExtension = filesExtension;
//            return this;
//        }

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

        public WeatherIntegrator build() {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(Builder builder) {
        //filesPath = builder.filesPath;
        gribFilesFolderPath = builder.gribFilesFolderPath;
        numberOfColumnDate = builder.numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        dateFormat = builder.dateFormat;

        //filesExtension = builder.filesExtension;
        gribFilesExtension = builder.gribFilesExtension;
        separator = builder.separator;
        lruCacheMaxEntries = builder.lruCacheMaxEntries;
        useIndex = builder.useIndex;

        lruCacheManager = LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension),
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, builder.variables, separator);
    }

    public String integrateLine(String line, ){


        String dataToBeIntegrated = lruCacheManager.getData(dateFormat.parse(separatedLine[numberOfColumnDate - 1]), latitude, longitude);
    }

    private void clearExportingDirectory() {
        //delete existing exported files on the export path
        if (Files.exists(Paths.get(filesExportPath))) {
            Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
        }
    }

    @Override
    public void fileParse(Path filePath) {

        try {
            fos = new FileOutputStream(filesExportPath + File.separator + filePath.toString().substring(filesPath.length()), true);
            //fos = new FileOutputStream(filesExportPath + File.separator + filePath.toString().substring(filesPath.length() + 1), true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            osw = new OutputStreamWriter(fos, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        bw = new BufferedWriter(osw);
        pw = new PrintWriter(bw, true);

        pw.write("company;vehicle;localDate;engineStatus;driver;driverEvent;longitude;latitude;altitude;angle;speed;odometer;satellites;fuelLevelLt;countryCode;rpm;levelType;fuelTankSize;vehicleOdometer;fuelConsumed;engineHours;closeToGasStation;deviceType;VehicleType;fuelRawValue;Per_cent_frozen_precipitation_surface;Precipitable_water_entire_atmosphere_single_layer;Precipitation_rate_surface_3_Hour_Average;Storm_relative_helicity_height_above_ground_layer;Total_precipitation_surface_3_Hour_Accumulation;Categorical_Rain_surface_3_Hour_Average;Categorical_Freezing_Rain_surface_3_Hour_Average;Categorical_Ice_Pellets_surface_3_Hour_Average;Categorical_Snow_surface_3_Hour_Average;Convective_Precipitation_Rate_surface_3_Hour_Average;Convective_precipitation_surface_3_Hour_Accumulation;U-Component_Storm_Motion_height_above_ground_layer;V-Component_Storm_Motion_height_above_ground_layer;Geopotential_height_highest_tropospheric_freezing;Relative_humidity_highest_tropospheric_freezing;Ice_cover_surface;Snow_depth_surface;Water_equivalent_of_accumulated_snow_depth_surface;Wind_speed_gust_surface;u-component_of_wind_maximum_wind;u-component_of_wind_height_above_ground;v-component_of_wind_maximum_wind;v-component_of_wind_height_above_ground;Total_cloud_cover_low_cloud_3_Hour_Average" + "\r\n");

    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
        try {
                WeatherIntegrator.numberofRecords++;

                long startTime = System.nanoTime();

                String dataToBeIntegrated = lruCacheManager.getData(dateFormat.parse(separatedLine[numberOfColumnDate - 1]), latitude, longitude);

                long endTime = System.nanoTime();

                //pw.write(line  + dataToBeIntegrated + "\r\n");

                pw.write(line.substring(0, line.length() - 1) + dataToBeIntegrated + "\r\n");


        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void emptySpatioTemporalInformation(Path file, String line) {
        //pw.write(line + new String(new char[lruCacheManager.getNumberOfVariables()]).replace("\0", separator) + "\r\n");

    }

    @Override
    public void outOfRangeSpatialInformation(Path file, String line) {
        //pw.write(line + new String(new char[lruCacheManager.getNumberOfVariables()]).replace("\0", separator) + "\r\n");

    }

    @Override
    public void afterLineParse() {
        try {

            pw.close();
            bw.close();
            osw.close();
            fos.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void integrateData(String filesExportPath) {

        this.filesExportPath = filesExportPath;

        clearExportingDirectory();

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(filesExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //create subdirectories in Export Directory if exist
        try (Stream<String> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension)).map(p -> p.getParent().toString().substring(filesPath.length())).distinct()) {

            stream.forEach(subdirectory ->
            {
                try {
                    Files.createDirectories(Paths.get(filesExportPath + subdirectory));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
    }

    public static Builder newWeatherIntegrator(String filesPath, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) {
        return new WeatherIntegrator.Builder(filesPath, gribFilesFolderPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat, variables);
    }


}
