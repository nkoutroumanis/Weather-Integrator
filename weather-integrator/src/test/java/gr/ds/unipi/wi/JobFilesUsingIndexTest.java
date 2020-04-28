package gr.ds.unipi.wi;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobFilesUsingIndexTest {

    @Test
    public void main() throws Exception {

        Stream<String> stream = Files.lines(Paths.get("./src/test/resources/weather-attributes/weather-attributes.txt"));
        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/csv-enriched/", true);

        WeatherIntegrator.newWeatherIntegrator(rp,
                "./src/test/resources/gribFiles/grib003Files/", stream.collect(Collectors.toList())).filter(Rectangle.newRectangle(-180, -90, 180, 90)).removeLastValueFromRecords()
                .useIndex().lruCacheMaxEntries(1).gribFilesExtension(".grb2").build().integrate(fileOutput);


    }
}