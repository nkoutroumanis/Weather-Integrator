package com.github.nkoutroumanis.weatherIntegrator.grib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dia.HDFSRandomAccessFile;
import org.joda.time.DateTime;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class GribFilesTree {

    private final TreeMap gribFilesTreeMap;
    private final String gribFilesFolderPath;
    private final String gribFilesExtension;

    private GribFilesTree(String gribFilesFolderPath, String gribFilesExtension) throws IOException {
        this.gribFilesTreeMap = new TreeMap<Long, String>();
        this.gribFilesFolderPath = gribFilesFolderPath;
        this.gribFilesExtension = gribFilesExtension;

        if(gribFilesFolderPath.startsWith("hdfs")){
            traverseGribFilesFromHDFS(new Path(gribFilesFolderPath), FileSystem.get(new Configuration()));
        }
        else{
            traverseFolder(gribFilesFolderPath);
        }

    }

    public static GribFilesTree newGribFilesTree(String gribFilesFolderPath, String gribFilesExtension) throws IOException {
        return new GribFilesTree(gribFilesFolderPath, gribFilesExtension);
    }

    public String getFilePathByUnixTime(long date) {

        Map.Entry tmsmp1 = gribFilesTreeMap.floorEntry(date);
        Map.Entry tmsmp2 = gribFilesTreeMap.ceilingEntry(date);

        try {
            if (Long.compare(Math.abs(date - (long) tmsmp1.getKey()), Math.abs(date - (long) tmsmp2.getKey())) == -1) {
                return (String) tmsmp1.getValue();
            } else {
                return (String) tmsmp2.getValue();
            }
        } catch (NullPointerException e) {//if floorEntry or ceiling entry does not exist
            if (tmsmp1 != null) {
                return (String) tmsmp1.getValue();
            } else {
                return (String) tmsmp2.getValue();
            }
        }
    }

    private void traverseGribFilesFromHDFS(Path filePath, FileSystem fs) throws IOException {
        List<String> pathsOfGribFiles = getAllPathsOfGribFilesFromHDFS(filePath,fs);
        pathsOfGribFiles.forEach(gribFilePath->{
            gribFilesTreeMap.put(getTimeOfGribFile(gribFilePath), gribFilePath);
        });
    }

    private List<String> getAllPathsOfGribFilesFromHDFS(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllPathsOfGribFilesFromHDFS(fileStat.getPath(), fs));
            } else {
                if(fileStat.getPath().toString().endsWith(gribFilesExtension)){
                    fileList.add(fileStat.getPath().toString());
                }
            }
        }
        return fileList;
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

    private long getTimeOfGribFile(String completeFilename) {
        NetcdfFile ncf = null;
        try {

            //ncf = NetcdfFile.openInMemory(completeFilename+"/d",Files.readAllBytes(Paths.get(completeFilename)));
            //ncf = NetcdfFile.op.openInMemory(completeFilename);
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

}
