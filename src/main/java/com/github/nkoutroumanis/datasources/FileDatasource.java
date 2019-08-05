package com.github.nkoutroumanis.datasources;

import com.github.nkoutroumanis.parsers.VfiObjectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileDatasource implements Datasource {

    private static final Logger logger = LoggerFactory.getLogger(FileDatasource.class);

    private final String directoryName;
    private final String filesExtension;

    private Stream<Path> filesStream;
    private Iterator<Path> filesIter;
    private Stream<String> linesStream;
    private Iterator<String> linesIter;
    private String filePath;

    private FileDatasource(String directoryName, String filesExtension) throws IOException {
        this.directoryName = directoryName;
        this.filesExtension = filesExtension;

        if (!directoryName.substring(directoryName.length() - 1).equals(File.separator)) {
            directoryName = directoryName + File.separator;
        }

        filesStream = Files.walk(Paths.get(directoryName)).filter(path -> path.getFileName().toString().endsWith(filesExtension));
        filesIter = filesStream.iterator();

        Path path = filesIter.next();
        filePath = path.toString();

        logger.info("File opened {}",filePath);

        linesStream = Files.lines(path);
        linesIter = linesStream.iterator();
    }

    public static FileDatasource newFileDatasource(String directoryName, String filesExtension) throws IOException {
        return new FileDatasource(directoryName, filesExtension);
    }

    @Override
    public String[] nextLine() {
        return new String[]{linesIter.next(), filePath.substring(directoryName.length())};
    }

    @Override
    public boolean hasNextLine() throws IOException {
        if (linesIter.hasNext()) {
            return true;
        } else {
            linesStream.close();
            if (filesIter.hasNext()) {

                Path path = filesIter.next();
                filePath = path.toString();

                logger.info("File opened {}", filePath);

                linesStream = Files.lines(path);
                linesIter = linesStream.iterator();
                return true;
            } else {
                filePath = null;
                filesStream.close();
            }
        }
        return false;
    }

    @Override
    public Datasource cloneDatasource() throws IOException {
        return newFileDatasource(directoryName, filesExtension);
    }
}
