package com.github.nkoutroumanis.parsers;

import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import com.typesafe.config.*;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JsonRecordParserTest {

    @Test
    public void recordParser() {
//
//        Type listType = new TypeToken<List<String>>() {}.getType();
//
        //List<String[]> s = gson.fromJson("{ \"id\" : 3 }", new TypeToken<List<String[]>>(){}.getType());

//        JSONArray rootOfPage =  new JSONArray("");
//
//        JsonParser parser = new JsonParser();
//        JsonObject o = parser.parse().getAsJsonObject();
//
//        JsonParserFactory factory=JsonParserFactory.getInstance();
//        JSONParser parser=factory.newJsonParser();
//        Map jsonMap=parser.parseJson(jsonString);


        Config config = ConfigFactory.parseString("{ \"id\" : 3, \"loc\" :{ \"xs\" : 3, \"dfdf\" : { \"ds\": 434} } }");

        Map<String, Object> properties = new HashMap<>();

        for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
            //String[] keys = ConfigUtil.splitPath(entry.getKey()).toArray(new String[0]);
            System.out.println("Root key = " + entry.getKey() + " " + entry.getValue().render());
            properties.put(entry.getKey(), entry.getValue().render());

        }


        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(properties));

        Config config1 = ConfigFactory.parseMap(properties);
        System.out.println(config1.root().render(ConfigRenderOptions.concise()));



        /*.root()
                .forEach((b1,b2) -> {
            System.out.println(b1 +" "+b2);
        });*/

        //s.forEach(k-> System.out.println(k));


    }
}