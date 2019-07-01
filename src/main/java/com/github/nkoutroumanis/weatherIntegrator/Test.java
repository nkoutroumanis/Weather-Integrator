package com.github.nkoutroumanis.weatherIntegrator;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import ucar.unidata.io.RandomAccessFile;

public class Test extends RandomAccessFile {
//    public Test() throws URISyntaxException, IOException {
//
//
//        FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
//        URL.setURLStreamHandlerFactory(factory);
//
//        String filePath = "mitsos.grib";
//        URI uri = new URI("hdfs://" + filePath);
//        URL fileURL = new URL(uri.getScheme(), uri.getHost(), uri.getPort(), filePath);
//        InputStream is = fileURL.openStream();
//    }

    protected FSDataInputStream hdfsFile;
    protected OutputStream hdfsOutFile;
    protected Path hdfsFP;
    protected Path hdfsOutFP;
    protected DistributedFileSystem hdfs;
    public boolean isHDFSIn = false;
    public boolean isHDFSOut = false;
    protected BufferedWriter hdfsOutBR;


    boolean bufferModified = false;

    public org.apache.hadoop.conf.Configuration hdfsConf;

    ///////////////////////////////////////////////////////////////////////
    // debug leaks - keep track of open files
    static protected boolean debugLeaks = false;
    static protected Set<String> allFiles = null;
    static protected List<String> openFiles = Collections.synchronizedList(new ArrayList<String>());   // could keep map on file hashcode
    static private AtomicLong count_openFiles = new AtomicLong();
    static private AtomicInteger maxOpenFiles = new AtomicInteger();


    public Test(String location, String mode, int bufferSize) throws IOException {





        if (bufferSize < 0) bufferSize = defaultBufferSize;
        this.location = location;
        if (debugLeaks) {
            allFiles.add(location);
        }
        if ((isHDFSIn || location.startsWith("hdfs")) && !location.contains("out") ) {
            //System.out.println( "[SAMAN][RandomAccessFile] HDFS IN" );
            isHDFSIn = true;
            //System.out.println("[net] opening hdfs file "+location);
            hdfs = new DistributedFileSystem();
            hdfsFP = new Path(StringUtils.unEscapeString(location));
            //System.out.println("[net] file path "+hdfsFP);
            org.apache.hadoop.conf.Configuration.addDefaultResource("core-site.xml");
            org.apache.hadoop.conf.Configuration.addDefaultResource("mapred-site.xml");
            org.apache.hadoop.conf.Configuration.addDefaultResource("hdfs-site.xml");
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            hdfs.initialize(hdfsFP.toUri(),conf);
            hdfsFile = hdfs.open(hdfsFP,4096);

            //hdfsFile.close();
            //System.out.println("[net] file opened successfully");

        }else if((isHDFSOut || location.startsWith("hdfs")) && location.contains("out")){
            //System.out.println( "[SAMAN][RandomAccessFile] HDFS OUT" );
            isHDFSOut = true;
            hdfs = new DistributedFileSystem();
            hdfsOutFP = new Path(StringUtils.unEscapeString(location));
            org.apache.hadoop.conf.Configuration.addDefaultResource("core-site.xml");
            org.apache.hadoop.conf.Configuration.addDefaultResource("mapred-site.xml");
            org.apache.hadoop.conf.Configuration.addDefaultResource("hdfs-site.xml");
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            hdfs.initialize(hdfsOutFP.toUri(),conf);

            hdfsOutFile = hdfs.create( hdfsOutFP );

            //hdfsOutBR = new BufferedWriter(new OutputStreamWriter(hdfsOutFile));

        } else
            try {
                this.file = new java.io.RandomAccessFile(location, mode);
            } catch (IOException ioe) {
                if (ioe.getMessage().equals("Too many open files")) {
                    //System.out.printf("RandomAccessFile %s%n", ioe);
                    try {
                        Thread.currentThread().sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    this.file = new java.io.RandomAccessFile(location, mode); // Windows having troublke keeping up ??
                } else {
                    throw ioe;
                }
            }

        this.readonly = mode.equals("r");
        init(bufferSize);

        if (debugLeaks) {
            openFiles.add(location);
            int max = Math.max(openFiles.size(), maxOpenFiles.get());
            maxOpenFiles.set(max);
            count_openFiles.getAndIncrement();
            if (showOpen) System.out.println("  open " + location);
            if (openFiles.size() > 1000)
                System.out.println("RandomAccessFile debugLeaks");
        }
    }

    public void flush() throws IOException {

        //System.out.println( "[SAMAN][RandomAccessFile][Flush()] location="+this.getLocation() );

        if( isHDFSIn ){
            //System.out.println("[net] not modified or is hdfs");
        }else if( isHDFSOut ){
            //hdfsOutBR.flush();
            //System.out.println("[SAMAN][RandomAccessFile][Flush()] isHDFSOut");

            //String temp = new String(buffer);
            //hdfsOutBR.write(temp.toCharArray(), 0, dataSize);

            //System.out.println( "[SAMAN][RandomAccessFile][flush] hdfsOut, flusing with size="+dataSize );
            ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
            org.apache.hadoop.io.IOUtils.copyBytes(bis,hdfsOutFile,(long)dataSize,false);
            bufferModified = false;
        }
        else{
            //System.out.println( "[SAMAN][RandomAccessFile][flush] file out, seek bufferstart="+bufferStart+", dataSize="+dataSize );
            file.seek(bufferStart);
            file.write(buffer, 0, dataSize);
            //System.out.println("--flush at "+bufferStart+" dataSize= "+dataSize+ " filePosition= "+filePosition);
            bufferModified = false;
        }

    }

    public void writeBytes(byte b[], int off, int len) throws IOException {
        // If the amount of data is small (less than a full buffer)...
        //System.out.println("[net] file write...");
        //System.out.println( "[SAMAN][RandomAccessFile][writeBytes(byte[],int,int] location="+this.getLocation()+",off="+off+",len="+len );
        if (len < buffer.length) {

            // If any of the data fits within the buffer...
            int spaceInBuffer = 0;
            int copyLength = 0;
            if (filePosition >= bufferStart) {
                spaceInBuffer = (int) ((bufferStart + buffer.length) - filePosition);
            }

            if (spaceInBuffer > 0) {
                // Copy as much as possible to the buffer.
                copyLength = (spaceInBuffer > len) ? len : spaceInBuffer;
                System.arraycopy(b, off, buffer, (int) (filePosition - bufferStart), copyLength);
                bufferModified = true;
                long myDataEnd = filePosition + copyLength;
                dataEnd = (myDataEnd > dataEnd) ? myDataEnd : dataEnd;
                dataSize = (int) (dataEnd - bufferStart);
                filePosition += copyLength;
                ///System.out.println("--copy to buffer "+copyLength+" "+len);
            }

            // If there is any data remaining, move to the new position and copy to
            // the new buffer.
            if (copyLength < len) {
                //System.out.println("--need more "+copyLength+" "+len+" space= "+spaceInBuffer);
                seek(filePosition);   // triggers a flush
                System.arraycopy(b, off + copyLength, buffer, (int) (filePosition - bufferStart), len - copyLength);
                bufferModified = true;
                long myDataEnd = filePosition + (len - copyLength);
                dataEnd = (myDataEnd > dataEnd) ? myDataEnd : dataEnd;
                dataSize = (int) (dataEnd - bufferStart);
                filePosition += (len - copyLength);
            }
            //System.out.println( "[SAMAN][RandomAccessFile][WriteBytes(byte,int,int)]" +
            //        ",filePosition="+filePosition+",bufferStart="+bufferStart+",dataSize="+dataSize+
            //        ",dataEnd="+dataEnd);

            // ...or write a lot of data...
        } else {

            // Flush the current buffer, and write this data to the file.
            if (isHDFSOut) {
                if (bufferModified) {
                    flush();
                }
                //String temp = new String(b);
                //hdfsOutBR.write(temp.toCharArray(), off, len);
                ByteArrayInputStream bis = new ByteArrayInputStream(b);
                org.apache.hadoop.io.IOUtils.copyBytes(bis,hdfsOutFile,(long)len,false);

                filePosition += len;
                bufferStart = filePosition;  // an empty buffer
                dataSize = 0;
                dataEnd = bufferStart + dataSize;

                //System.out.println( "[SAMAN][RandomAccessFile][WriteBytes] hdfs filePosition="+filePosition
                //        +",bufferStart="+bufferStart
                //        +",dataSize="+dataSize
                //        +",dataEnd="+dataEnd );

            } else {
                if (bufferModified) {
                    flush();
                }
                file.seek(filePosition);  // moved per Steve Cerruti; Jan 14, 2005
                file.write(b, off, len);
                //System.out.println("--write at "+filePosition+" "+len);

                filePosition += len;
                bufferStart = filePosition;  // an empty buffer
                dataSize = 0;
                dataEnd = bufferStart + dataSize;
                //System.out.println( "[SAMAN][RandomAccessFile][WriteBytes(byte,int,int)]" +
                //        ",filePosition="+filePosition+",bufferStart="+bufferStart+",dataSize="+dataSize+
                //        ",dataEnd="+dataEnd);
                //System.out.println( "[SAMAN][RandomAccessFile][WriteBytes] filePosition="+filePosition+",bufferStart="
                //        +bufferStart+",dataSize="+dataSize+",dataEnd="+dataEnd );

            }
        }
    }

    private void init(int bufferSize) {
        // Initialise the buffer
        bufferStart = 0;
        dataEnd = 0;
        dataSize = 0;
        filePosition = 0;
        buffer = new byte[bufferSize];
        endOfFile = false;
    }


}
