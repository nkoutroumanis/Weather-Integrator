package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExperimentsJob {

    public static void main(String args[]){


        MongoCredential credential = MongoCredential.createCredential("myUserAdmin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().maxConnectionIdleTime(90000).build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("83.212.102.163", 28017), credential, options);

        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        int k = 34210;
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

                System.out.println("determined radius "+ determinedRadius);


                MongoCursor<Document> cursor = m.aggregate(Arrays.asList(Document.parse( "{ $geoNear: { near: {type: \"Point\", coordinates: ["+randomX+", "+randomY+"]}," +
                        "key: \"location\" ," + "maxDistance: "+ determinedRadius * 1000 +" ," + "distanceField: \"distance\" ," + "spherical: true" + "} }"),Document.parse("{ $count: \"count\" }"))).iterator();


//        try {
//            while (cursor.hasNext()) {
//                System.out.println(cursor.next().toJson());
//            }
//        } finally {
//            cursor.close();
//        }



                System.out.println("the returned (larger or equal than k) "+cursor.next().getInteger("count"));



                cursor = m.aggregate(Arrays.asList(Document.parse( "{ $geoNear: { near: {type: \"Point\", coordinates: ["+randomX+", "+randomY+"]}," +
                        "key: \"location\" ," + "maxDistance: "+ determinedRadius * 1000  +"," + "num: "+ k +" ," + "distanceField: \"distance\" ," + "spherical: true" + "} }"),Document.parse("{ $group: { _id: null, dist: { $last: \"$distance\" } } }"))).iterator();



//        try {
//            while (cursor.hasNext()) {
//                System.out.println(cursor.next().toJson());
//            }
//        } finally {
//            cursor.close();
//        }



                System.out.println("the k distance "+cursor.next().getDouble("dist"));


//            });


//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }

}
