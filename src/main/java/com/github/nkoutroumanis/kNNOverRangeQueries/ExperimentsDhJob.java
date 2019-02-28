package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.github.nkoutroumanis.FilesParse;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExperimentsDhJob {

    public static void main(String args[]) throws IOException {

        if(Integer.valueOf(args[0])==0){
            doOperations("real","/home/nikolaos/Documents/thesis-dataset/","/home/nikolaos/Documents/greek-hist/thesis-dataset/",200);
        }

        if(Integer.valueOf(args[0])==1){
            doOperations("synthetic1","/home/nikolaos/Documents/synthetic-dataset1/","/home/nikolaos/Documents/greek-hist/synthetic-dataset1/", 50);
        }

        if(Integer.valueOf(args[0])==2){
            doOperations("synthetic2","/home/nikolaos/Documents/synthetic-dataset2/","/home/nikolaos/Documents/greek-hist/synthetic-dataset2/", 50);
        }


    }

    private static void doOperations(String database, String filesPath, String histogramsPath, int points) throws IOException {

        final String filesExtension = ".csv";
        final String separator = ";";
        final int numberOfColumnLongitude = 2;
        final int numberOfColumnLatitude = 3;
        final int numberOfColumnDate = 4;


        MongoCredential credential = MongoCredential.createCredential(database, database, database.toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().maxConnectionIdleTime(90000000/*90000*/).build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase(database).getCollection("geoPoints");


        if(histogramsPath.equals(""))
        {
            System.out.println("histogramsPath is Null");
        }
        Random r = new Random();


        List<Path> files = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension)).collect(Collectors.toList());
        final int numberOfFiles = files.size();

        System.out.println("files path loaded");

        List<File> subfolder = Arrays.asList(new File(histogramsPath).listFiles(File::isDirectory));

        subfolder.forEach(path -> {

            System.out.println(path);

            LoadHistogram lh = LoadHistogram.newLoadHistogram(path.toString());
            RadiusDetermination rd = RadiusDetermination.newRadiusDetermination(lh.getHistogram(), lh.getNumberOfCellsxAxis(), lh.getNumberOfCellsyAxis(), lh.getMinx(), lh.getMiny(), lh.getMaxx(), lh.getMaxy());


            Stream.of(1500, 800, 300, 100, 50, 10).forEach(ki -> {

                Stream.of(/*0.1, 0.05, 0.01, 0.005, 0.001*/0).forEach(dh->{

                    final int k = ki;
                    //int points = 200;

                    List<Long> timeForRadiusDetermination = new ArrayList<>();

                    List<Double> resultsRatio = new ArrayList<>();
                    List<Double> radiusRatio = new ArrayList<>();

                    List<Long> timeOfCountQuery = new ArrayList<>();
                    List<Long> timeOfRealRadius = new ArrayList<>();


                    for (int i = 0; i < points; i++) {


                        double longitude = -1000;
                        double latitude = -1000;

                        int b = 0;
                        while(b==0){

                            try {
                                int randomFile = r.nextInt(numberOfFiles);
                                List<String> lines = Files.lines(files.get(randomFile)).collect(Collectors.toList());
                                int randomLine = r.nextInt(lines.size());
                                String line = lines.get(randomLine);

                                String[] separatedLine = line.split(separator);

                                if (FilesParse.empty.test(separatedLine[numberOfColumnLongitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnLatitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnDate - 1])) {
                                    continue;
                                }

                                longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                                latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                                if (FilesParse.longitudeInGreekRegion.test(longitude) && FilesParse.latitudeInGreekRegion.test(latitude)) {
                                    //this block had only b = 1;

                                    b =1;
//                                double x = (lh.getMaxx() - lh.getMinx()) / lh.getNumberOfCellsxAxis();
//                                double y = (lh.getMaxy() - lh.getMiny()) / lh.getNumberOfCellsyAxis();
//
//                                long xc = (long) (longitude / x);
//                                long yc = (long) (latitude / y);
//
//                                if (lh.getHistogram().containsKey(xc + (yc * lh.getNumberOfCellsxAxis()))) {
//                                    if(lh.getHistogram().get(xc + (yc * lh.getNumberOfCellsxAxis()))<k){
//                                        System.out.println("cell has "+lh.getHistogram().get(xc + (yc * lh.getNumberOfCellsxAxis())) +"points");
//                                        b = 1;
//                                    }
//                                    else{
//                                        //System.out.println("Repeating Point Generation");
//                                    }
//                                } else {
//                                    try {
//                                        throw new Exception("the histogam cell does not exist");
//                                    } catch (Exception e) {
//                                        e.printStackTrace();
//                                    }
//                                }

                                }

                            } catch (ArrayIndexOutOfBoundsException | IOException e) {
                                System.out.println("Exeption while generating random point ");
                                System.out.println(e);
                                System.out.println("Repeating the generation");
                            }
                        }

                        double randomX;
                        double randomY;

                        if(r.nextInt(2)==1){
                            randomX = (longitude + dh);
                        }
                        else{
                            randomX = (longitude - dh);
                        }

                        if(r.nextInt(2)==1){
                            randomY = (latitude + dh);
                        }
                        else{
                            randomY = (latitude - dh);
                        }



//        if(r.nextInt(1)==1){
//            randomX = (longitude - dh) + ((longitude + dh) - (longitude - dh)) * r.nextDouble();
//        }
//        else{
//            randomX = (longitude - dh) + ((longitude + dh) - (longitude - dh)) * r.nextDouble();
//        }
//
//        if(r.nextInt(1)==1){
//            randomY = (latitude - dh) + ((latitude + dh) - (latitude - dh)) * r.nextDouble();
//        }
//        else{
//            randomY = (latitude - dh) + ((latitude + dh) - (latitude - dh)) * r.nextDouble();
//        }


                        //System.out.println("Point: " + randomX + "  " + randomY);

                        long t1 = System.nanoTime();
                        double determinedRadius = rd.findRadius(randomX, randomY, Long.valueOf(k));
                        timeForRadiusDetermination.add(System.nanoTime() - t1);
                        System.out.println("formed point ("+ randomX +", " + randomY+"), km= "+determinedRadius+", i="+i);


                        long t3 = System.nanoTime();
                        MongoCursor<Document> cursor2 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                                "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000) + " ," + "distanceField: \"distance\" ," + "spherical: true, num:" + k + "} }"), Document.parse("{ $group: { _id:null, theLast:{ $last:\"$distance\" } } }"))).iterator();
                        double realRadius = cursor2.next().getDouble("theLast");
                        timeOfRealRadius.add(System.nanoTime() - t3);

                        if(Double.isInfinite(((determinedRadius * 1000) - realRadius) / realRadius)){
                            i--;
                            continue;
                        }

                        radiusRatio.add(((determinedRadius * 1000) - realRadius) / realRadius);//(r' - r)/r
                        System.out.println("The last: "+ realRadius + "The ratio "+ (((determinedRadius * 1000) - realRadius) / realRadius));
                        cursor2.close();

                        if (radiusRatio.get(radiusRatio.size() - 1) < 0) {
                            try {
                                System.out.println("Error: " + radiusRatio.get(resultsRatio.size() - 1));
                                throw new Exception("Negative numbers are added in the list");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }


                        long t2 = System.nanoTime();
                        MongoCursor<Document> cursor1 = m.aggregate(Arrays.asList(Document.parse("{ $match: { location: { $geoWithin : { $centerSphere : [ [" + randomX + ", " + randomY + "], " + (determinedRadius / 6378.1) + " ] } } } }"), Document.parse("{ $count: \"count\" }"))).iterator();
                        resultsRatio.add(((double) (cursor1.next().getInteger("count") - k) / k));//(n' - n)/n
                        timeOfCountQuery.add(System.nanoTime() - t2);
                        cursor1.close();

                        if (resultsRatio.get(resultsRatio.size() - 1) < 0) {
                            try {
                                System.out.println("Error: " + resultsRatio.get(resultsRatio.size() - 1));
                                throw new Exception("Negative numbers are added in the list");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                    }

                    LongSummaryStatistics tss = timeForRadiusDetermination.stream().mapToLong(Long::valueOf).summaryStatistics();
                    double tsum = 0;
                    for (Long e : timeForRadiusDetermination) {
                        tsum = tsum + Math.pow(e.doubleValue() - tss.getAverage(), 2);
                    }
                    double tssStd = Math.sqrt(tsum / (timeForRadiusDetermination.size() - 1));

                    DoubleSummaryStatistics ress = resultsRatio.stream().mapToDouble(Double::valueOf).summaryStatistics();
                    double resum = 0;
                    for (Double e : resultsRatio) {
                        resum = resum + Math.pow(e - ress.getAverage(), 2);
                    }
                    double ressStd = Math.sqrt(resum / (resultsRatio.size() - 1));

                    DoubleSummaryStatistics rass = radiusRatio.stream().mapToDouble(Double::valueOf).summaryStatistics();
                    double rasum = 0;
                    for (Double e : radiusRatio) {
                        rasum = rasum + Math.pow(e - rass.getAverage(), 2);
                    }
                    double rassStd = Math.sqrt(rasum / (radiusRatio.size() - 1));


                    LongSummaryStatistics tss1 = timeOfCountQuery.stream().mapToLong(Long::valueOf).summaryStatistics();
                    double t1sum = 0;
                    for (Long e : timeOfCountQuery) {
                        t1sum = t1sum + Math.pow(e.doubleValue() - tss1.getAverage(), 2);
                    }
                    double tss1Std = Math.sqrt(t1sum / (timeOfCountQuery.size() - 1));


                    LongSummaryStatistics tss2 = timeOfRealRadius.stream().mapToLong(Long::valueOf).summaryStatistics();
                    double t2sum = 0;
                    for (Long e : timeOfRealRadius) {
                        t2sum = t2sum + Math.pow(e.doubleValue() - tss2.getAverage(), 2);
                    }
                    double tss2Std = Math.sqrt(t2sum / (timeOfRealRadius.size() - 1));


                    try (FileOutputStream fos = new FileOutputStream(path + File.separator + "Experiments_k_" + k + "_dh_"+dh+ "+.txt", true);
                         OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)) {

                        pw.write("For k=" + k + " of Histogram " + path + "\r\n");
                        pw.write("\r\n");
                        pw.write("Determination of Radius Average Time (ns): " + tss.getAverage() + "\r\n");
                        pw.write("Determination of Radius Max Time (ns): " + tss.getMax() + "\r\n");
                        pw.write("Determination of Radius Min Time (ns): " + tss.getMin() + "\r\n");
                        pw.write("Determination of Radius Std of Time: " + tssStd + "\r\n");
                        pw.write("\r\n");

                        pw.write("Count Query Average Time (ns): " + tss1.getAverage() + "\r\n");
                        pw.write("Count Query Max Time (ns): " + tss1.getMax() + "\r\n");
                        pw.write("Count Query Min Time (ns): " + tss1.getMin() + "\r\n");
                        pw.write("Count Query Std of Time: " + tss1Std + "\r\n");
                        pw.write("\r\n");

                        pw.write("Query for Real Radius Average Time (ns): " + tss2.getAverage() + "\r\n");
                        pw.write("Query for Real Radius Max Time (ns): " + tss2.getMax() + "\r\n");
                        pw.write("Query for Real Radius Min Time (ns): " + tss2.getMin() + "\r\n");
                        pw.write("Query for Real Radius Std of Time: " + tss2Std + "\r\n");
                        pw.write("\r\n");

                        pw.write("Average Results Ratio: " + ress.getAverage() + "\r\n");
                        pw.write("Max Results Ratio: " + ress.getMax() + "\r\n");
                        pw.write("Min Results Ratio: " + ress.getMin() + "\r\n");
                        pw.write("Std of Ratio: " + ressStd + "\r\n");
                        pw.write("\r\n");

                        pw.write("Average Radius Ratio: " + rass.getAverage() + "\r\n");
                        pw.write("Max Radius Ratio: " + rass.getMax() + "\r\n");
                        pw.write("Min Radius Ratio: " + rass.getMin() + "\r\n");
                        pw.write("Std of Radius: " + rassStd + "\r\n");
                        pw.write("\r\n");


                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                });
            });

        });
        mongoClient.close();
    }
//
//    public static double findStdOfList(Stream<Double> stream){
//
//    }

}
