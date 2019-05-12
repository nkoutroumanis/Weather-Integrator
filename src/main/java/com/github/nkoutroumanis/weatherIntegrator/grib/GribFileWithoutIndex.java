package com.github.nkoutroumanis.weatherIntegrator.grib;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class GribFileWithoutIndex implements GribFile {

    private final List<Variable> listOfVariables;

    private GribFileWithoutIndex(String path, List<String> listOfVariables) throws IOException {
        NetcdfFile ncf = NetcdfFile.open(path);
        this.listOfVariables = listOfVariables.stream().map(s -> ncf.findVariable(s)).collect(Collectors.toList());
    }

    public static GribFileWithoutIndex newGribFileWithoutIndex(String path, List<String> listOfVariables) throws IOException {
        return new GribFileWithoutIndex(path, listOfVariables);
    }

    public List<String> getDataValuesByLatLon(double lat, double lon) {

        List<String> values = new ArrayList();

        listOfVariables.forEach(v -> {
            try {
                try {
                    values.add(String.valueOf(v.read("0,0, " + GribFile.getLatIndex(lat) + ", " + GribFile.getLonIndex(lon))).replace(" ", ""));
                } catch (InvalidRangeException i) {
                    try {
                        values.add(String.valueOf(v.read("0, " + GribFile.getLatIndex(lat) + ", " + GribFile.getLonIndex(lon))).replace(" ", ""));
                    } catch (InvalidRangeException k) {
                        values.add(String.valueOf(v.read()));
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return values;
    }
}
