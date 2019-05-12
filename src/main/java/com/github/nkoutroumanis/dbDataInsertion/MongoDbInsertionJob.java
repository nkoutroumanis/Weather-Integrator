package com.github.nkoutroumanis.dbDataInsertion;

import com.github.nkoutroumanis.datasources.FileDatasource;

public final class MongoDbInsertionJob {

    public static void main(String args[]) throws Exception {

//        MongoDbConnector connector = MongoDbConnector.newMongoDbConnector("localhost", 27017,"synthetic1", "myUserAdmin", "abc123");
//        long t = System.currentTimeMillis();
//
//        MongoDbDataInsertion.newMongoDbDataInsertion(connector, "/home/nikolaos/Documents/synthetic-dataset/", 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
//
//        System.out.println((System.currentTimeMillis()-t)/1000);

        MongoDbConnector connector1 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "real", "real", "real");
        long t1 = System.currentTimeMillis();
        MongoDbDataInsertion.newMongoDbDataInsertion(connector1, FileDatasource.newFileDatasource("/home/nikolaos/Documents/thesis-dataset/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
        System.out.println("real: " + (System.currentTimeMillis() - t1) / 1000);

        MongoDbConnector connector2 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "synthetic1", "synthetic1", "synthetic1");
        long t2 = System.currentTimeMillis();
        MongoDbDataInsertion.newMongoDbDataInsertion(connector2, FileDatasource.newFileDatasource("/home/nikolaos/Documents/synthetic-dataset1/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
        System.out.println("syn1: " + (System.currentTimeMillis() - t2) / 1000);

        MongoDbConnector connector3 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "synthetic2", "synthetic2", "synthetic2");
        long t3 = System.currentTimeMillis();
        MongoDbDataInsertion.newMongoDbDataInsertion(connector3, FileDatasource.newFileDatasource("/home/nikolaos/Documents/synthetic-dataset2/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
        System.out.println("syn2: " + (System.currentTimeMillis() - t3) / 1000);
    }

}
