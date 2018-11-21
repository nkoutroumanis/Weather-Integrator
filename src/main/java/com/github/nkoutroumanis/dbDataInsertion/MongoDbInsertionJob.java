package com.github.nkoutroumanis.dbDataInsertion;

public final class MongoDbInsertionJob {

    public static void main(String args[]) {

        MongoDbConnector connector = MongoDbConnector.newMongoDbConnector("83.212.102.163", 27017, "myUserAdmin", "abc123", "test");
        MongoDbDataInsertion.newMongoDbDataInsertion(connector, "", 7, 8, 3, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");

    }

}
