package gr.ds.unipi.wi.grib;

import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GribFileWithIndex implements GribFile {

    private final List<Map.Entry<Array, Index>> listOfEntries;

    //private static long sum = 0;
    //private static long count = 0;

    private GribFileWithIndex(String path, List<String> listOfVariables, Function<String, NetcdfFile> netcdfFileFunction) throws IOException {

//        long start = System.currentTimeMillis();

        NetcdfFile ncf = netcdfFileFunction.apply(path);

        this.listOfEntries = listOfVariables.stream().map(s -> {
            Array array = null;
            try {
                array = ncf.findVariable(s).read();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return new AbstractMap.SimpleEntry<>(array, array.getIndex());
        }).collect(Collectors.toList());

//        long end = System.currentTimeMillis();
//        sum = sum + (end - start);
//        count++;
//
//        System.out.println("WEATHER FILE TIME OPENING: " + (end - start) + " Average: " + ((double)sum)/count);

    }

    public static GribFileWithIndex newGribFileWithIndex(String path, List<String> listOfVariables, Function<String, NetcdfFile> netcdfFileFunction) throws IOException {
        return new GribFileWithIndex(path, listOfVariables, netcdfFileFunction);
    }

    public List<Object> getDataValuesByLatLon(double lat, double lon) {

        List<Object> values = new ArrayList();

        listOfEntries.forEach(e -> {

            try {
                values.add(e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon))))/*.toString()*/);
            } catch (ArrayIndexOutOfBoundsException k) {
//                try {
                values.add(e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon))))/*.toString()*/);
//                } catch (ArrayIndexOutOfBoundsException j) {
//                    values.add(e.getKey().copy());
//                }
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

        listOfEntries.forEach(e -> {

            Object v1;
            Object v2;
            Object v3;
            Object v4;

//            Object v;

            try {
                v1 = e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0), GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0))));
                v2 = e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(Math.floor(lat * 2) / 2.0), GribFile.getLonIndex(Math.floor(lon * 2) / 2.0))));
                v3 = e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(Math.floor(lat * 2) / 2.0), GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0))));
                v4 = e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0), GribFile.getLonIndex(Math.floor(lon * 2) / 2.0))));

//                v = e.getKey().getObject((e.getValue().set(0, 0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon))))/*.toString()*/;

            } catch (ArrayIndexOutOfBoundsException k) {
                v1 = e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0), GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0))));
                v2 = e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(Math.floor(lat * 2) / 2.0), GribFile.getLonIndex(Math.floor(lon * 2) / 2.0))));
                v3 = e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(Math.floor(lat * 2) / 2.0), GribFile.getLonIndex(Math.ceil(lon * 2) / 2.0))));
                v4 = e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(Math.ceil(lat * 2) / 2.0), GribFile.getLonIndex(Math.floor(lon * 2) / 2.0))));

//                v = e.getKey().getObject((e.getValue().set(0, GribFile.getLatIndex(lat), GribFile.getLonIndex(lon))))/*.toString()*/;
            }

            values.add(((float)v1*(1/d1) + (float)v2*(1/d2) + (float)v3*(1/d3) + (float)v4*(1/d4)) / ( (1/d1) + (1/d2) + (1/d3) + (1/d4) ));
//            values.add(Math.abs((((float)v1*(1/d1) + (float)v2*(1/d2) + (float)v3*(1/d3) + (float)v4*(1/d4)) / ( (1/d1) + (1/d2) + (1/d3) + (1/d4) ))-(float)v));

        });
        return values;
    }
}
