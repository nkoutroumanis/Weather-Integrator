package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.Job;
import org.joda.time.DateTime;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

public final class GribFilesTree {

    private final TreeMap gribFilesTreeMap;
    private final String gribFilesFolderPath;
    private final String gribFilesExtension;

    private GribFilesTree(String gribFilesFolderPath, String gribFilesExtension) {
        this.gribFilesTreeMap = new TreeMap<Long, String>();
        this.gribFilesFolderPath = gribFilesFolderPath;
        this.gribFilesExtension = gribFilesExtension;
        traverseFolder(gribFilesFolderPath);
    }

    public String getFilePathByUnixTime(long date) {
        return (String) gribFilesTreeMap.floorEntry(date).getValue();
    }

    private void traverseFolder(String gribFilesFolderPath) {
        File folder = new File(gribFilesFolderPath);

        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                String filename = listOfFiles[i].getName();
                if (filename.endsWith(gribFilesExtension)) {
                    String completeFilename = gribFilesFolderPath + File.separator + filename;
                    long time = getTimeOfGribFile(completeFilename);
                    gribFilesTreeMap.put(time, completeFilename);
                }
            } else if (listOfFiles[i].isDirectory()) {
                traverseFolder(gribFilesFolderPath + File.separator + listOfFiles[i].getName());
            }
        }
    }

//    private static TreeMap<Long, NetcdfFile> traverseFolder(String folderName) throws IOException {
//        File folder = new File(folderName);
//        File[] listOfFiles = folder.listFiles();
//        System.out.println("Number of Files:"+listOfFiles.length);
//        TreeMap<Long, NetcdfFile> result = new TreeMap<Long, NetcdfFile>();
//        for (int i = 0; i < listOfFiles.length; i++) {
//            NetcdfFile ncf;
//
//            String filename = listOfFiles[i].getAbsolutePath();
//            if (listOfFiles[i].isFile()) {
//                if( filename.endsWith(targetExtension)) {
//                    ncf = NetcdfFile.open(filename);
//                    long time = getTime(ncf);//long time = new Random().nextLong() * new Random().nextLong() *  new Random().nextLong() +10000L ;
//                    result.put(time, ncf);
//                }
//            } else if(listOfFiles[i].isDirectory()) {
//                result.putAll(traverseFolder(filename));
//            }
//        }
//        System.out.println("loaded " + result.size() + " grib files");
//        return result;
//    }

    private long getTimeOfGribFile(String completeFilename) {
        NetcdfFile ncf = null;
        try {
            ncf = NetcdfFile.open(completeFilename);


        } catch (IOException e) {
            e.printStackTrace();
        }
        Variable timeVariable = ncf.findVariable("time");

        float time_val = 0;
        try {
            time_val = (60 * 60) * ((float) timeVariable.readScalarDouble());
        } catch (IOException e) {
            e.printStackTrace();
        }

        String timeUnits = timeVariable.getUnitsString();
        String strToRemove = "Hour since ";
        String strConverted = timeUnits.substring(strToRemove.length());
        try {
            ncf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        DateTime date = DateTime.parse(strConverted);
        long unixTime = date.getMillis() / 1000;
        return (unixTime + (long) time_val);
    }

    public static GribFilesTree newGribFilesTree(String gribFilesFolderPath, String gribFilesExtension) {
        return new GribFilesTree(gribFilesFolderPath, gribFilesExtension);
    }

//    private long getTime(String completeFilename) throws IOException
//    {
//        NetcdfFile ncf = NetcdfFile.open(completeFilename);
//        Variable timeVariable = ncf.findVariable("time");
//        float time_val = (60*60)* ((float)timeVariable.readScalarDouble());
//        String timeUnits = timeVariable.getUnitsString();
//        String strToRemove = "Hour since ";
//        String strConverted = timeUnits.substring(strToRemove.length());
//        long unixTime = getUnixTime(strConverted);
//        return (unixTime + (long)time_val);
//    }

//    private long getUnixTime(String dateTime) {
//        try {
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
//            return sdf.parse(getLiteralValue(dateTime).replace("T", " ")).getTime()/1000l;
//        }catch(Exception e) {
//            e.printStackTrace();
//        }
//        return Long.MIN_VALUE;
//    }

//    public static String getLiteralValue(String l) {
//        String r = l.split("\\^\\^")[0];
//        if(r.startsWith("\"")&&r.endsWith("\""))
//            return r.substring(1, r.length()-1);
//        return r;
//    }

}
