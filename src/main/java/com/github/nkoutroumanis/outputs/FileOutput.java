package com.github.nkoutroumanis.outputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class FileOutput implements Output<String> {

    private static final Logger logger = LoggerFactory.getLogger(FileOutput.class);
    private final String directory;
    private String filePath = "";
    private FileOutputStream fos;
    private OutputStreamWriter osw;
    private BufferedWriter bw;
    private PrintWriter pw;

    private FileOutput(String directory, boolean deleteDirectoryIfExist) {

        if (!directory.substring(directory.length() - 1).equals(File.separator)) {
            directory = directory + File.separator;
        }

        this.directory = directory;

        if (deleteDirectoryIfExist) {
            deleteDirectory(new File(directory));
        }

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static FileOutput newFileOutput(String directory, boolean deleteDirectoryIfExist) {
        return new FileOutput(directory, deleteDirectoryIfExist);
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    @Override
    public void out(String line, String lineMetaData) {

        if (!lineMetaData.equals(filePath)) {

            close();

            try {

                logger.info("Create directory on {}", getDirectory() + lineMetaData.substring(0, lineMetaData.lastIndexOf(File.separator) + 1));
                //System.out.println("create directory on " + );
                Files.createDirectories(Paths.get(getDirectory() + lineMetaData.substring(0, lineMetaData.lastIndexOf(File.separator) + 1)));

                fos = new FileOutputStream(getDirectory() + lineMetaData, true);
                osw = new OutputStreamWriter(fos, "utf-8");
                bw = new BufferedWriter(osw);
                pw = new PrintWriter(bw, true);

                filePath = lineMetaData;

                System.out.println("lineWithMeta meta:" + getDirectory());

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        pw.write(line + "\r\n");

    }

    @Override
    public void close() {
        if (fos != null) {

            try {
                pw.close();
                bw.close();
                osw.close();
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getDirectory() {
        return directory;
    }
}
