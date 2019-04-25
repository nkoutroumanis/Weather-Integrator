package com.github.nkoutroumanis;

import java.io.*;

public class FileOutput implements Output{

    private final String directory;

    private String filePath = "";

    private FileOutputStream fos;
    private OutputStreamWriter osw;
    private BufferedWriter bw;
    private PrintWriter pw;

    private FileOutput(String directory){
        this.directory = directory;

        if(!directory.substring(directory.length()-1).equals(File.separator)){
            directory = directory + File.separator;
        }
    }

    public static FileOutput newFileOutput(String directory){
        return new FileOutput(directory);
    }

    @Override
    public void out(String line, String lineMeta) {

        if(!lineMeta.equals(filePath)){

            if(fos != null) {

                try {
                    pw.close();
                    bw.close();
                    osw.close();
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {

                fos = new FileOutputStream(directory + lineMeta, true);
                osw = new OutputStreamWriter(fos, "utf-8");
                bw = new BufferedWriter(osw);
                pw = new PrintWriter(bw, true);

                filePath = lineMeta;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

        }

        pw.write(line + "\r\n");

    }
}
