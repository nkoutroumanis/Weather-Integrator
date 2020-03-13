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
}
