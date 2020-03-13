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

        long start = System.currentTimeMillis();

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
}
