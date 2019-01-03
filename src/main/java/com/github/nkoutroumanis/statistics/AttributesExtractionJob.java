package com.github.nkoutroumanis.statistics;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AttributesExtractionJob {

    public static void main(String[] args) {

        String csvPath = "/Users/nicholaskoutroumanis/Downloads/POI/Helpers/Company Function.csv";
        String separator = ";";
        int columnNumber = 2;
        String exportTxtPath = "/Users/nicholaskoutroumanis/Downloads/POI/Helpers/Company_Function_attr.txt";

        try (Stream<String> stream = Files.lines(Paths.get(csvPath));
             FileOutputStream fos = new FileOutputStream(exportTxtPath, true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
             BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)){

            List<String> attributes = new ArrayList<>();

            for(String s :stream.collect(Collectors.toList())){
                if(s.isEmpty()){
                    continue;
                }
                try{
                    attributes.add(s.split(separator)[columnNumber-1]);
                }
                catch(ArrayIndexOutOfBoundsException k){
                    continue;
                }
            }

            attributes.stream().distinct().forEach(p ->{
                pw.write(p + "\r\n");
            });



        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
