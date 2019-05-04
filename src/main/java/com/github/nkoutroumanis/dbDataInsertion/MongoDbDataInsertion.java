package com.github.nkoutroumanis.dbDataInsertion;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.Parser;
import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.checkSpatioTemporalInfo.CheckSpatioTemporalInfo;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public final class MongoDbDataInsertion {

    private final MongoDbConnector mongoDbConnector;
    private final String database;
    private final Parser parser;
    private final int numberOfColumnLongitude;
    private final int numberOfColumnLatitude;
    private final int numberOfColumnDate;
    private final DateFormat dateFormat;

    private final String separator;
    private final Rectangle rectangle;

    private List<Document> docs;
    private MongoCollection mongoCollection;

    public static class Builder {

        private final MongoDbConnector mongoDbConnector;
        private final String database;
        private final Parser parser;
        private final int numberOfColumnLongitude;
        private final int numberOfColumnLatitude;
        private final int numberOfColumnDate;
        private final DateFormat dateFormat;

        private String separator = ";";
        private Rectangle rectangle = Rectangle.newRectangle(-180,-90,180,90);


        public Builder(MongoDbConnector mongoDbConnector, Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {
            this.mongoDbConnector = mongoDbConnector;
            this.database = mongoDbConnector.getDatabase();
            this.parser = parser;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnDate = numberOfColumnDate;
            this.dateFormat = new SimpleDateFormat(dateFormat);

        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder filter(Rectangle rectangle){
            this.rectangle = rectangle;
            return this;
        }

        public MongoDbDataInsertion build() {
            return new MongoDbDataInsertion(this);
        }
    }

    private MongoDbDataInsertion(Builder builder) {
        mongoDbConnector = builder.mongoDbConnector;
        database = builder.database;
        parser = builder.parser;
        numberOfColumnLongitude = builder.numberOfColumnLongitude;
        numberOfColumnLatitude = builder.numberOfColumnLatitude;
        numberOfColumnDate = builder.numberOfColumnDate;
        dateFormat = builder.dateFormat;

        separator = builder.separator;
        rectangle = builder.rectangle;
    }


    public static Builder newMongoDbDataInsertion(MongoDbConnector mongoDbConnector, Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) throws Exception {
        return new MongoDbDataInsertion.Builder(mongoDbConnector, parser, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }

    public void insertDataOnCollection(String collection) throws IOException {

        mongoCollection = mongoDbConnector.getMongoClient().getDatabase(database).getCollection(collection);

        docs = new ArrayList<>();

        while (parser.hasNextLine()){

            try {
                String[] a = parser.nextLine();

                String line = a[0];
                String[] separatedLine = line.split(separator);

                if (Parser.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnLatitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnDate - 1])) {
                    continue;
                }

                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);
                Date d = dateFormat.parse(separatedLine[numberOfColumnDate - 1]);

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                    continue;
                }

                //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
                docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", d));
                //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));

                //System.out.println(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
                //mongoCollection.insertOne(docs);

                if(docs.size() == 3000){
                    mongoCollection.insertMany(docs);
                    docs = new ArrayList<>();
                }

            }
            catch(ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e){
                continue;
            }

        }

        if(docs.size() != 0){
            mongoCollection.insertMany(docs);
            docs = null;
        }

        mongoCollection = null;

        mongoDbConnector.getMongoClient().close();
    }

//    @Override
//    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
//
//        try {
//            //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
//            Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//            docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
//            //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));
//
//            //System.out.println(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
//            //mongoCollection.insertOne(docs);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//    }

//    @Override
//    public void afterLineParse() {
//        if (docs.size() > 0) {
//            mongoCollection.insertMany(docs);
//        }
//    }

}
