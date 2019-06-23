package com.github.nkoutroumanis.outputs;

import com.github.nkoutroumanis.dbDataInsertion.MongoDbConnector;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MongoOutput implements Output<Document> {

    private static final Logger logger = LoggerFactory.getLogger(MongoOutput.class);
    private final MongoClient mongoClient;
    private final MongoCollection<Document> mongoCollection;
    private final ArrayList<Document> buffer;
    private final int batchSize;

    public MongoOutput(String host, int port, String database, String username, String password, String collection, int batchSize) {
        logger.info(password);
        this.mongoClient = MongoDbConnector.newMongoDbConnector(host, port, database, username, password).getMongoClient();
        this.mongoCollection = this.mongoClient.getDatabase(database).getCollection(collection);
        this.batchSize = batchSize;
        this.buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void out(Document document, String lineMetaData) {

        buffer.add(document);
        //buffer.add(Document.parse(line));
        if (buffer.size() == batchSize) {
            logger.debug("Writing batch to Mongo...");
            mongoCollection.insertMany(buffer);
            logger.debug("Done!");
            buffer.clear();
        }
    }

    @Override
    public void close() {
        if (buffer.size() > 0) {
            logger.debug("Writing batch to Mongo...");
            mongoCollection.insertMany(buffer);
            buffer.clear();
        }
        mongoClient.close();
    }
}
