package com.github.nkoutroumanis.kNNOverRangeQueries.gridPartitioning;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.*;
import java.util.Map;

public class LoadHistogram {

    private final Map<Long, Long> histogram;

    private final long numberOfCellsxAxis;
    private final long numberOfCellsyAxis;

    private final double minx;
    private final double miny;
    private final double maxx;
    private final double maxy;

    private LoadHistogram(String path) {

        Config conf = ConfigFactory.parseFile(new File(path + File.separator + "properties.json"));

        this.numberOfCellsxAxis = conf.getLong("cellsInXAxis");
        this.numberOfCellsyAxis = conf.getLong("cellsInYAxis");
        this.minx = conf.getDouble("space2D.minx");
        this.miny = conf.getDouble("space2D.miny");
        this.maxx = conf.getDouble("space2D.maxx");
        this.maxy = conf.getDouble("space2D.maxy");

        FileInputStream fis = null;
        ObjectInputStream ois = null;
        Map<Long, Long> histogram = null;
        try {
            fis = new FileInputStream(path + File.separator + "histogram.ser");
            ois = new ObjectInputStream(fis);

            histogram = (Map<Long, Long>) ois.readObject();
            ois.close();
            fis.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.histogram = histogram;

        System.out.println("histogram Loaded");

    }

    public static LoadHistogram newLoadHistogram(String path) {
        return new LoadHistogram(path);
    }

    public Map<Long, Long> getHistogram() {
        return histogram;
    }

    public long getNumberOfCellsxAxis() {
        return numberOfCellsxAxis;
    }

    public long getNumberOfCellsyAxis() {
        return numberOfCellsyAxis;
    }

    public double getMinx() {
        return minx;
    }

    public double getMiny() {
        return miny;
    }

    public double getMaxx() {
        return maxx;
    }

    public double getMaxy() {
        return maxy;
    }
}
