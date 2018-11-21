package com.github.nkoutroumanis.dbDataInsertion;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDbConnector {

    private final MongoClient mongoClient;
    private final String database;

    private MongoDbConnector(String host, int port, String username, String password, String database) {

        MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
        mongoClient = new MongoClient(new ServerAddress(host, port), credential, options);

        this.database = database;
    }

    public String getDatabase(){
        return database;
    }

    public MongoClient getMongoClient(){
        return mongoClient;
    }

    public static MongoDbConnector newMongoDbConnector(String host, int port, String username, String password, String database) {
        return new MongoDbConnector(host, port, username, password, database);
    }


}
