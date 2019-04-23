package com.github.nkoutroumanis.integrator.weatherIntegrator.grib;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class GribFileWithIndex implements GribFile {

    private final List<Map.Entry<Array, Index>> listOfEntries;

    private GribFileWithIndex(String path, List<String> listOfVariables) throws IOException {

        NetcdfFile ncf = NetcdfFile.open(path);

        this.listOfEntries = listOfVariables.stream().map(s -> {
            Array array = null;
            try {
                array = ncf.findVariable(s).read();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return new AbstractMap.SimpleEntry<>(array, array.getIndex());
        }).collect(Collectors.toList());

    }

    public static GribFileWithIndex newGribFileWithIndex(String path, List<String> listOfVariables) throws IOException {
        return new GribFileWithIndex(path, listOfVariables);
    }

    public List<String> getDataValuesByLatLon(double lat, double lon) {

        List<String> values = new ArrayList();

        listOfEntries.forEach(e -> {
            double t1 = System.nanoTime();
            try {
                values.add(e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon)))).toString());
            } catch (ArrayIndexOutOfBoundsException k) {
                try {
                    values.add(e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon)))).toString());
                } catch (ArrayIndexOutOfBoundsException j) {
                    values.add(e.getKey().copy().toString());
                }
            }

        });

        return values;
    }
}
