package com.github.nkoutroumanis.kNNOverRangeQueries;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class ExperimentsJob {

    public static void main(String args[]) {

        MongoCredential credential = MongoCredential.createCredential("myUserAdmin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().maxConnectionIdleTime(9000000/*90000*/).build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("83.212.102.163", 28017), credential, options);

        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");
        Random r = new Random();

        DecimalFormat dec = new DecimalFormat("#0.00");

        List<File> subfolder = Arrays.asList(new File(args[0]/*"/Users/nicholaskoutroumanis/Desktop/untitled/"*/).listFiles(File::isDirectory));

        subfolder.forEach(path -> {

            System.out.println(path);

            LoadHistogram lh = LoadHistogram.newLoadHistogram(path.toString());
            RadiusDetermination rd = RadiusDetermination.newRadiusDetermination(lh.getHistogram(), lh.getNumberOfCellsxAxis(), lh.getNumberOfCellsyAxis(), lh.getMinx(), lh.getMiny(), lh.getMaxx(), lh.getMaxy());

            final int k = Integer.valueOf(args[1]);
            int points = 10;

            List<Long> timeForRadiusDetermination = new ArrayList<>();
            List<Double> resultsRatio = new ArrayList<>();
            List<Double> radiusRatio = new ArrayList<>();

            for (int i = 0; i < points; i++) {

                double randomX = lh.getMinx() + ((lh.getMaxx() - 0.1d) - lh.getMinx()) * r.nextDouble();
                double randomY = lh.getMiny() + ((lh.getMaxy() - 0.1d) - lh.getMiny()) * r.nextDouble();

                randomX = Double.parseDouble(dec.format(randomX));
                randomY = Double.parseDouble(dec.format(randomY));

                System.out.println("Point: " + randomX + "  " + randomY);

                long t1 = System.currentTimeMillis();
                double determinedRadius = rd.findRadius(randomX, randomY, Long.valueOf(k));
                timeForRadiusDetermination.add(System.currentTimeMillis() - t1);

                System.out.println("Radius: " + determinedRadius);

                MongoCursor<Document> cursor1 = m.aggregate(Arrays.asList(Document.parse("{ $match: { location: { $geoWithin : { $centerSphere : [ [" + randomX + ", " + randomY + "], " + (determinedRadius / 6378.1) + " ] } } } }"), Document.parse("{ $count: \"count\" }"))).iterator();
                resultsRatio.add(((double) (cursor1.next().getInteger("count") - k) / k));//(n' - n)/n
                cursor1.close();
                System.out.println(resultsRatio.size() + " Count Finished");

                if (resultsRatio.get(resultsRatio.size() - 1) < 0) {
                    try {
                        System.out.println("Error: " + resultsRatio.get(resultsRatio.size() - 1));
                        throw new Exception("Negative numbers are added in the list");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                MongoCursor<Document> cursor2 = m.aggregate(Arrays.asList(Document.parse("{ $geoNear: { near: {type: \"Point\", coordinates: [" + randomX + ", " + randomY + "]}," +
                        "key: \"location\" ," + "maxDistance: " + (((determinedRadius)) * 1000) + " ," + "distanceField: \"distance\" ," + "spherical: true, num:" + k + "} }"), Document.parse("{ $group: { _id:null, theLast:{ $last:\"$distance\" } } }"))).iterator();

                double realRadius = cursor2.next().getDouble("theLast");

                radiusRatio.add(((determinedRadius * 1000) - realRadius) / realRadius);//(r' - r)/r
                cursor2.close();
                System.out.println("Radius Finished");

                if (radiusRatio.get(radiusRatio.size() - 1) < 0) {
                    try {
                        System.out.println("Error: " + radiusRatio.get(resultsRatio.size() - 1));
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

            try (FileOutputStream fos = new FileOutputStream(path + File.separator + "Experiments_k_" + k + "+.txt", true);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)) {

                pw.write("For k=" + k + " of Histogram " + path + "\r\n");
                pw.write("\r\n");
                pw.write("Determination of Radius Average Time (ms): " + tss.getAverage() + "\r\n");
                pw.write("Determination of Radius Max Time (ms): " + tss.getMax() + "\r\n");
                pw.write("Determination of Radius Min Time (ms): " + tss.getMin() + "\r\n");
                pw.write("Determination of Radius Std of Time: " + tssStd + "\r\n");
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
        mongoClient.close();
    }
}
