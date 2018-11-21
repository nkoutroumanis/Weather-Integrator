package com.github.nkoutroumanis.dbDataInsertion;

public class DataInsertionJob {

    public static void main(String args[]){
        MongoDbDataInsertion.
        newMongoDbDataInsertion().insertDataOnCollection()("geoPoints",
                "/home/nikolaos/Documents/eraseItt", ";", 4,
                3, 2, "yyyy-MM-dd HH:mm:ss");
    }

}
