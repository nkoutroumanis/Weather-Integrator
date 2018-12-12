package com.github.nkoutroumanis.histogram;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class HistogramCreationJobTest {

    @Test
    public void main() {

        Space2D space2D = Space2D.newSpace2D(-26.7, 0, 122.56, 60.93);
        Map<String,Object> m = new HashMap<>();
        m.put("space2D",space2D);
        m.put("cellsInXAxis",1234);
        m.put("cellsInYAxis",2347656765l);




        try (Writer writer = new FileWriter("./files_for_test/example.json")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            gson.toJson(m, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}