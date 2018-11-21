package com.github.nkoutroumanis.dbDataInsertion;

import com.github.nkoutroumanis.FilesParse;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public final class MongoDbDataInsertion implements FilesParse {

    private final MongoDbConnector mongoDbConnector;
    private final String database;
    private final String filesPath;
    private final int numberOfColumnLongitude;
    private final int numberOfColumnLatitude;
    private final int numberOfColumnDate;
    private final DateFormat dateFormat;

    private final String filesExtension;
    private final String separator;

    private List<Document> docs;
    private MongoCollection mongoCollection;

    public static class Builder {

        private final MongoDbConnector mongoDbConnector;
        private final String database;
        private final String filesPath;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;
        private final DateFormat dateFormat;

        private String filesExtension = ".csv";
        private String separator = ";";

        public Builder(MongoDbConnector mongoDbConnector, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {
            this.mongoDbConnector = mongoDbConnector;
            this.database = mongoDbConnector.getDatabase();
            this.filesPath = filesPath;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;
            this.dateFormat = new SimpleDateFormat(dateFormat);

        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public MongoDbDataInsertion build() {
            return new MongoDbDataInsertion(this);
        }
    }

    private MongoDbDataInsertion(Builder builder) {
        mongoDbConnector = builder.mongoDbConnector;
        database = builder.database;
        filesPath = builder.filesPath;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnDate = builder.numberOfColumnDate;
        dateFormat = builder.dateFormat;

        filesExtension = builder.filesExtension;
        separator = builder.separator;
    }


    public static Builder newMongoDbDataInsertion(MongoDbConnector mongoDbConnector, String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {
        return new MongoDbDataInsertion.Builder(mongoDbConnector, filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }

    public void insertDataOnCollection(String collection) {

        mongoCollection = mongoDbConnector.getMongoClient().getDatabase(database).getCollection(collection);

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);

        mongoCollection = null;

        mongoDbConnector.getMongoClient().close();
    }

    @Override
    public void fileParse(Path filePath) {
        docs = new ArrayList<>();
    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, float longitude, float latitude) {
        try {
            //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
            Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
            docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterLineParse() {
        if (docs.size() > 0) {
            mongoCollection.insertMany(docs);
        }
    }

}
