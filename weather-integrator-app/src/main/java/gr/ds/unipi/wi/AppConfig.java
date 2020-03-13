package gr.ds.unipi.wi;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.outputs.KafkaOutput;
import gr.ds.unipi.stpin.outputs.Output;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.JsonRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.stpin.parsers.VfiObjectParser;

import java.io.File;

public class AppConfig {
    //public static final Config config = ConfigFactory.load();
    private final Config config;

    private AppConfig(String pathOfConfigFile) {
        config = ConfigFactory.parseFile(new File(pathOfConfigFile));
    }

    public static AppConfig newAppConfig(String pathOfConfigFile) {
        return new AppConfig(pathOfConfigFile);
    }

    public Datasource getDataSource() throws Exception {
        Config datasource = config.getConfig("datasource");
        Datasource ds;

        if (datasource.getString("type").equals("files")) {
            Config files = datasource.getConfig("files");
            ds = FileDatasource.newFileDatasource(files.getString("filesPath"), files.getString("filesExtension"));
        } else if (datasource.getString("type").equals("kafka")) {
            Config kafka = datasource.getConfig("kafka");
            ds = KafkaDatasource.newKafkaDatasource(kafka.getString("consumerPropertiesPath"), kafka.getString("consumerTopic"), kafka.getInt("poll"));
        } else {
            throw new Exception("datasource type is not set correctly");
        }

        return ds;
    }

    public Output getOutput() throws Exception {
        Config output = config.getConfig("output");
        Output o;

        if (output.getString("type").equals("files")) {
            Config files = output.getConfig("files");
            o = FileOutput.newFileOutput(files.getString("filesOutputPath"), files.getBoolean("deleteOutputDirectoryIfExists"));
        } else if (output.getString("type").equals("kafka")) {
            Config kafka = output.getConfig("kafka");
            o = KafkaOutput.newKafkaOutput(kafka.getString("producerPropertiesPath"), kafka.getString("producerTopic"));
        } else {
            throw new Exception("output type is not set correctly");
        }

        return o;
    }

    public RecordParser getRecordParser(Datasource datasource) throws Exception {
        Config parser = config.getConfig("parser");
        RecordParser rp;

        if (parser.getString("type").equals("csv")) {
            Config csv = parser.getConfig("csv");

            if (parser.hasPath("dateFormat")) {
                rp = new CsvRecordParser(datasource, csv.getString("separator"), csv.getInt("numberOfColumnLongitude"), csv.getInt("numberOfColumnLatitude"), csv.getInt("numberOfColumnDate"), parser.getString("dateFormat"));
            } else {
                rp = new CsvRecordParser(datasource, csv.getString("separator"), csv.getInt("numberOfColumnLongitude"), csv.getInt("numberOfColumnLatitude"));
            }

        } else if (parser.getString("type").equals("json")) {
            Config json = parser.getConfig("json");

            if (parser.hasPath("dateFormat")) {
                rp = new JsonRecordParser(datasource, json.getString("longitudeFieldName"), json.getString("latitudeFieldName"), json.getString("dateFieldName"), parser.getString("dateFormat"));
            } else {
                rp = new JsonRecordParser(datasource, json.getString("longitudeFieldName"), json.getString("latitudeFieldName"));
            }
        } else if (parser.getString("type").equals("vfi")) {
            rp = new VfiObjectParser(datasource);
        } else {
            throw new Exception("parser type is not set correctly");
        }

        return rp;
    }
}
