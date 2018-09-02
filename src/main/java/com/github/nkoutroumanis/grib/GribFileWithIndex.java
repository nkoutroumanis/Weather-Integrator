package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.Job;
import com.github.nkoutroumanis.WeatherIntegrator;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class GribFileWithIndex implements GribFile {

    private final List<Map.Entry<Array,Index>> listOfEntries;
    private final String separator;

    private GribFileWithIndex(String path, List<String> listOfVariables, String separator) throws IOException {

        NetcdfFile ncf = NetcdfFile.open(path);

        this.listOfEntries = listOfVariables.stream().map(s->{
            Array array = null;
            try {
                array = ncf.findVariable(s).read();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return new AbstractMap.SimpleEntry<>(array,array.getIndex());
        }).collect(Collectors.toList());

        this.separator = separator;
    }

    public static GribFileWithIndex newGribFileWithIndex(String path, List<String> listOfVariables, String separator) throws IOException {
        return new GribFileWithIndex(path, listOfVariables, separator);
    }

    public String getDataValuesByLatLon(float lat, float lon){
        StringBuilder s = new StringBuilder();

        listOfEntries.forEach(e->{

                s.append(separator);
            double t1 = System.nanoTime();
                s.append(e.getKey().getFloat(e.getValue().set(0, 0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon))));
            WeatherIntegrator.TEMPORARY_POINTER1 = (System.nanoTime()-t1);
            WeatherIntegrator.TEMPORARY_POINTER2++;
        });
        return s.toString();
    }


}