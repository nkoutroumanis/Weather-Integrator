package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.Job;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;


import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GribFile {

    private final String path;
    private final List<Variable> listOfVariables;

    private GribFile(String path, List<String> listOfVariables) throws IOException {
        this.path = path;
        NetcdfFile ncf = NetcdfFile.open(path);
        this.listOfVariables = listOfVariables.stream().map(s->ncf.findVariable(s)).collect(Collectors.toList());
    }

    public Stream<Variable> getVariablesAsStream(){
        return listOfVariables.stream();
    }

    public String getPath() {
        return path;
    }

    public String getDataValuesByLatLon(float lat, float lon){
        StringBuilder s = new StringBuilder();


        listOfVariables.forEach(v->{
            try {
                s.append(Job.SEPARATOR);
                double t1 = System.nanoTime();
                s.append(String.valueOf(v.read("0,0,"+getLatIndex(lat)+","+getLonIndex(lon))).replace(" ",""));
                Job.TEMPORARY_POINTER1 = (System.nanoTime() - t1);
                Job.TEMPORARY_POINTER2++;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidRangeException e) {
                e.printStackTrace();
            }
        });

        return s.toString();
    }

    public static GribFile newGribFile(String path, List<String> listOfVariables) throws IOException {
        return new GribFile(path, listOfVariables);
    }

    private static double roundToHalf(float f) {
        return Math.round(f * 2) / 2.0;
    }

    private static int getRoundedIndex(double x) {
        //first we check if the number is round
        boolean isRound = Math.ceil(x) == Math.floor(x);
        if(isRound)
        {
            return (int) (2*x);
        }
        else
        {
            return (int)( 2*Math.floor(x) +1);
        }
    }

    public static int getLatIndex(float fLat) { // lats: 90...-90 per 0.5 (361 values)
        double dLat = roundToHalf(fLat);
        int i = (int) (2 * (90 - dLat));
        return i;
    }

    public static int getLonIndex(float fLon) { // lons: 0...359.5, per 0.5  (720 values)
        double dLon = roundToHalf(fLon);
        int i = (int) (2 * dLon);
        return i;
    }



}
