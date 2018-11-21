package com.github.nkoutroumanis.weatherIntegrator.grib;

import com.github.nkoutroumanis.weatherIntegrator.WeatherIntegrator;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;


import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public final class GribFileWithoutIndex implements GribFile {

    private final List<Variable> listOfVariables;
    private final String separator;

    private GribFileWithoutIndex(String path, List<String> listOfVariables, String separator) throws IOException {
        NetcdfFile ncf = NetcdfFile.open(path);
        this.listOfVariables = listOfVariables.stream().map(s -> ncf.findVariable(s)).collect(Collectors.toList());
        this.separator = separator;
    }

    public static int i = 0;

    public String getDataValuesByLatLon(float lat, float lon) {
        StringBuilder s = new StringBuilder();


        listOfVariables.forEach(v -> {
            try {
                s.append(separator);
                double t1 = System.nanoTime();
                try {
                    s.append(String.valueOf(v.read("0,0," + GribFile.getLatIndex(lat) + "," + GribFile.getLonIndex(lon))).replace(" ", ""));
                } catch (InvalidRangeException i) {
                    s.append(String.valueOf(v.read()));
                }

                i++;
                WeatherIntegrator.TEMPORARY_POINTER1 = (System.nanoTime() - t1);
                WeatherIntegrator.TEMPORARY_POINTER2++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return s.toString();
    }

    public static GribFileWithoutIndex newGribFileWithoutIndex(String path, List<String> listOfVariables, String separator) throws IOException {
        return new GribFileWithoutIndex(path, listOfVariables, separator);
    }
}