package com.github.nkoutroumanis.integrator.weatherIntegrator;

import com.github.nkoutroumanis.FileParser;
import com.github.nkoutroumanis.Parser;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.List;

public class WeatherIntegrator {

    private final Parser parser;

    private final String gribFilesFolderPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;
    private final String separator;


    private final String gribFilesExtension;
    private final int lruCacheMaxEntries;
    private final boolean useIndex;


    public static class Builder {
        public Builder(Parser parser, String gribFilesFolderPath, int numberOfColumnDate, int numberOfColumnLongitude, int numberOfColumnLatitude, String dateFormat, List<String> variables) {

        }

    }

    public void startIntegration() throws IOException {

        while (parser.hasNextLine()){

             parser.nextLine();


        }

    }






        //private FileOutputStream fos;
    //private OutputStreamWriter osw;
    //private BufferedWriter bw;
    //private PrintWriter pw;

    //public static long hits = 0;
    //public static long numberofRecords = 0;


    public static void main(String args[]){


        WeatherDataObtainer wdo = WeatherDataObtainer.newWeatherDataObtainer("/.../.../", Arrays.asList("")).build();

        try {
            FileParser fp = new FileParser("/.../...",".csv");




            while (fp.hasNextLine()){

                if()
                fp.nextLine()
            }

        } catch (IOException e) {
            e.printStackTrace();
        }



        wdo.obtainAttributes();
    }
}
