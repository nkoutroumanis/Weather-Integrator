package com.github.nkoutroumanis;

import java.util.Arrays;

public final class Job {

    public static void main(String args[]) {
        WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
                "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
                9, 8, "dd/MM/yyyy hh:mm:ss",
                Arrays.asList("Relative_humidity_height_above_ground", "Temperature_height_above_ground"))
                .clearExportingFiles().useIndex().build().IntegrateData();
    }
}
