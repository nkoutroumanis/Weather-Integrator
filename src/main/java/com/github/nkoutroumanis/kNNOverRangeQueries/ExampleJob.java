package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.Arrays;

public class ExampleJob {

    public static void main(String args[]) {


        MongoCredential credential = MongoCredential.createCredential("myUserAdmin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().maxConnectionIdleTime(90000).build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("83.212.102.163", 28017), credential, options);

        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        //int k = 5;
        int k = 103024;
        System.out.println(k);
//        Random r = new Random();
//
//        double randomX = lh.getMinx() + ((lh.getMaxx() - 0.1d) - lh.getMinx()) * r.nextDouble();
//        double randomY = lh.getMiny() + ((lh.getMaxy() - 0.1) - lh.getMiny()) * r.nextDouble();
        double randomX = 23.709028;
        double randomY = 37.957193;

//        try {
//            Stream<Path> subfolder = Files.walk(Paths.get(""), 1).filter(Files::isDirectory);
//            subfolder.forEach(path -> {

        LoadHistogram lh = LoadHistogram.newLoadHistogram("/Users/nicholaskoutroumanis/Downloads/1/");
        RadiusDetermination rd = RadiusDetermination.newRadiusDetermination(lh.getHistogram(), lh.getNumberOfCellsxAxis(), lh.getNumberOfCellsyAxis(), lh.getMinx(), lh.getMiny(), lh.getMaxx(), lh.getMaxy());

        double determinedRadius = rd.findRadius(randomX, randomY, Long.valueOf(k));

        System.out.println("determined radius in Km " + determinedRadius);
        System.out.println("determined radius in m " + (determinedRadius * 1000l));


        //WILL BE USED
        MongoCursor<Document> cursor0 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000l) + " ," + "distanceField: \"distance\" ," + "spherical: true, num:" + k + "} }"), Document.parse("{ $group: { _id:null, theLast:{ $last:\"$distance\" } } }"))).iterator();

        try {
            while (cursor0.hasNext()) {
                System.out.println(cursor0.next().toJson());
            }
        } finally {
            cursor0.close();
        }

        MongoCursor<Document> cursor1 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000l) + " ," + "distanceField: \"distance\" ," + "spherical: true, num:" + k + "} }"))).iterator();

        try {
            while (cursor1.hasNext()) {
                System.out.println(cursor1.next().toJson());
            }
        } finally {
            cursor1.close();
        }

        System.out.println("------");


        MongoCursor<Document> cursor2 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000l) + " ," + "distanceField: \"distance\" ," + "spherical: true, limit:6" + "} }"))).iterator();

        try {
            while (cursor2.hasNext()) {
                System.out.println(cursor2.next().toJson());
            }
        } finally {
            cursor2.close();
        }

        System.out.println("------");


        MongoCursor<Document> cursor3 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000l) + " ," + "distanceField: \"distance\" ," + "spherical: true, limit:1000000" + "} }"), Document.parse("{ $count: \"count\" }"))).iterator();

        try {
            while (cursor3.hasNext()) {
                System.out.println(cursor3.next().toJson());
            }
        } finally {
            cursor3.close();
        }

//        MongoCursor<Document> cursor1 = m.aggregate(Arrays.asList(Document.parse( "{ $geoNear: { near: {type: \"Point\", coordinates: ["+randomX+", "+randomY+"]}," +
//                "key: \"location\" ," + "maxDistance: "+ ((( determinedRadius)) *1000l) +" ," + "distanceField: \"distance\" ," + "spherical: true, limit:1030244"+ "} }"),Document.parse("{ $count: \"count\" }"))).iterator();
//
//        try {
//            while (cursor1.hasNext()) {
//                System.out.println(cursor1.next().toJson());
//            }
//        } finally {
//            cursor1.close();
//        }


        MongoCursor<Document> cursor4 = m.aggregate(Arrays.asList(Document.parse("{ $match: { location: { $geoWithin : { $centerSphere : [ [" + randomX + ", " + randomY + "], " + ((int) determinedRadius / 6378.1) + " ] } } } }"), Document.parse("{ $count: \"count\" }"))).iterator();
        System.out.println("POINTS RETURNED " + cursor4.next().getInteger("count"));


//        try {
//            while (cursor2.hasNext()) {
//                System.out.println(cursor2.next().toJson());
//            }
//        } finally {
//
//            cursor2.close();
//        }


//        MongoCursor<Document> cursor2 = m.aggregate(Arrays.asList(Document.parse( "{ $geoNear: { near: {type: \"Point\", coordinates: ["+randomX+", "+randomY+"]}," +
//                        "key: \"location\" ," + "maxDistance: "+ determinedRadius * 1000  +"," + "num: "+ k +" ," + "distanceField: \"distance\" ," + "spherical: true" + "} }"),Document.parse("{ $group: { _id: null, dist: { $last: \"$distance\" } } }"))).iterator();
//
//
//
//        try {
//            while (cursor2.hasNext()) {
//                System.out.println(cursor2.next().toJson());
//            }
//        } finally {
//            cursor2.close();
//        }


//                System.out.println("the k distance "+cursor.next().getDouble("dist"));


//            });


//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }

}
