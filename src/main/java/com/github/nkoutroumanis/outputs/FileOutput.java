package com.github.nkoutroumanis.outputs;

import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class FileOutput implements Output {

    private final String directory;

    private String filePath = "";

    private final RecordParser recordParser;

    private FileOutputStream fos;
    private OutputStreamWriter osw;
    private BufferedWriter bw;
    private PrintWriter pw;

    private FileOutput(RecordParser recordParser, String directory, boolean deleteDirectoryIfExist) {
        this.recordParser = recordParser;
        this.directory = directory;

        if (!directory.substring(directory.length() - 1).equals(File.separator)) {
            directory = directory + File.separator;
        }

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

    public static FileOutput newFileOutput(RecordParser recordParser, String directory, boolean deleteDirectoryIfExist) {
        return new FileOutput(recordParser, directory, deleteDirectoryIfExist);
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
    public void out(Record record) {

        String lineMeta = record.getMetadata();

        if (!lineMeta.equals(filePath)) {

            close();

            try {

                System.out.println("create directory on " + directory + lineMeta.substring(0, lineMeta.lastIndexOf(File.separator) + 1));
                Files.createDirectories(Paths.get(directory + lineMeta.substring(0, lineMeta.lastIndexOf(File.separator) + 1)));

                fos = new FileOutputStream(directory + lineMeta, true);
                osw = new OutputStreamWriter(fos, "utf-8");
                bw = new BufferedWriter(osw);
                pw = new PrintWriter(bw, true);

                filePath = lineMeta;

                System.out.println("lineWithMeta meta:" + lineMeta);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        pw.write(recordParser.toCsv(record) + "\r\n");

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

}
