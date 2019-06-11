package com.github.nkoutroumanis.parsers;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class JsonRecordParserTest {

    @Test
    public void recordParser() {
        Gson gson = new Gson();
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

        ConfigFactory.parseString("{ \"id\" : 3, \"loc\" :{ \"xs\" : 3, \"dfdf\" : { \"ds\": 434} } }").root().forEach(
                (s1 , s2)->{

                    System.out.println(s1+" "+s2);

                    if(s2 instanceof ConfigV)

    });

        //s.forEach(k-> System.out.println(k));


    }
}