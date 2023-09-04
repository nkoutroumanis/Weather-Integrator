package gr.ds.unipi.wi.grib;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GribFileWithoutIndex implements GribFile {

    private final List<Variable> listOfVariables;

    private GribFileWithoutIndex(String path, List<String> listOfVariables, Function<String, NetcdfFile> netcdfFileFunction) throws IOException {
        NetcdfFile ncf = netcdfFileFunction.apply(path);
        this.listOfVariables = listOfVariables.stream().map(ncf::findVariable).collect(Collectors.toList());
    }

    public static GribFileWithoutIndex newGribFileWithoutIndex(String path, List<String> listOfVariables, Function<String, NetcdfFile> netcdfFileFunction) throws IOException {
        return new GribFileWithoutIndex(path, listOfVariables, netcdfFileFunction);
    }

    public List<Object> getDataValuesByLatLon(double lat, double lon) {

        List<Object> values = new ArrayList();

        listOfVariables.forEach(v -> {
            try {
                try {
                    values.add(v.read("0,0, " + GribFile.getLatIndex(lat) + ", " + GribFile.getLonIndex(lon)).getObject(0));
                } catch (InvalidRangeException i) {
                    try {
                        values.add(v.read("0, " + GribFile.getLatIndex(lat) + ", " + GribFile.getLonIndex(lon)).getObject(0));
                    } catch (InvalidRangeException e) {
                        e.printStackTrace();
//                        values.add(v.read().toString().replace(" ", ""));
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return values;
    }

    @Override
    public List<Object> getDataValuesByLatLonInterpolated(double lat, double lon) {

        List<Object> values = new ArrayList();

        double d1 = GribFile.haversine(Math.ceil(lon * 2) / 2.0,Math.ceil(lat * 2) / 2.0,lon,lat);
        double d2 = GribFile.haversine(Math.floor(lon * 2) / 2.0,Math.floor(lat * 2) / 2.0,lon,lat);
        double d3 = GribFile.haversine(Math.ceil(lon * 2) / 2.0,Math.floor(lat * 2) / 2.0,lon,lat);
        double d4 = GribFile.haversine(Math.floor(lon * 2) / 2.0,Math.ceil(lat * 2) / 2.0,lon,lat);

        if(Double.compare(d1,0) ==0 || Double.compare(d2,0) ==0 || Double.compare(d3,0) ==0 || Double.compare(d4,0) ==0){
            return getDataValuesByLatLon(lat,lon);
        }

        listOfVariables.forEach(v -> {

            Object v1 = 0;
            Object v2 = 0;
            Object v3 = 0;
            Object v4 = 0;

            try {
                try {
                    v1 = v.read("0,0, " + GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0)).getObject(0);
                    v2 = v.read("0,0, " + GribFile.getLatIndex(Math.floor(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.floor(lon * 2) / 2.0)).getObject(0);
                    v3 = v.read("0,0, " + GribFile.getLatIndex(Math.floor(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0)).getObject(0);
                    v4 = v.read("0,0, " + GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.floor(lon * 2) / 2.0)).getObject(0);

                } catch (InvalidRangeException i) {
                    try {
                        v1 = v.read("0, " + GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0)).getObject(0);
                        v2 = v.read("0, " + GribFile.getLatIndex(Math.floor(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.floor(lon * 2) / 2.0)).getObject(0);
                        v3 = v.read("0, " + GribFile.getLatIndex(Math.floor(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0)).getObject(0);
                        v4 = v.read("0, " + GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0) + ", " + GribFile.getLonIndex(Math.floor(lon * 2) / 2.0)).getObject(0);
                    }catch (InvalidRangeException e){
                        e.printStackTrace();
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            values.add(((float)v1*(1/d1) + (float)v2*(1/d2) + (float)v3*(1/d3) + (float)v4*(1/d4)) / ( (1/d1) + (1/d2) + (1/d3) + (1/d4) ));
        });
        return values;
    }
}
