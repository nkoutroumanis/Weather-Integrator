package com.github.nkoutroumanis.dbDataInsertion;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public final class MongoDbConnector {

    private final MongoClient mongoClient;
    private final String database;

    private MongoDbConnector(String host, int port, String database, String username, String password) {
        MongoCredential credential = MongoCredential.createCredential(username, "admin", password.toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().maxConnectionIdleTime(90000)/*.sslEnabled(true)*/.build();
        mongoClient = new MongoClient(new ServerAddress(host, port), credential, options);

        this.database = database;
    }

    public static MongoDbConnector newMongoDbConnector(String host, int port, String database, String username, String password) {
        return new MongoDbConnector(host, port, database, username, password);
    }

    public String getDatabase() {
        return database;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }


}
