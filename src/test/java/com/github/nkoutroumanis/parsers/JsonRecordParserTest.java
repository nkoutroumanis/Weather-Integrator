package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.kafkaToMongoDB.Consts;
import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import com.typesafe.config.*;
import org.bson.Document;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
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




        String s1 = "{\"_id\":{\"$oid\":\"5ce941147e49752dfd8923b0\"},\"VOUCHER_ID\":448903,\"VEHICLE_ID\":1497033,\"RECORD_TYPE\":\"E\",\"TIMESTAMP\":{\"$date\":1512737792000},\"LOCAL_TIME\":true,\"LATITUDE\":{\"$numberLong\":\"43957152\"},\"LONGITUDE\":{\"$numberLong\":\"11137166\"},\"location\":{\"lon\":11.137165999999999,\"lat\":43.957152},\"SPEED\":28,\"HEADING\":44,\"GPS_QUALITY\":3,\"STATUS\":1,\"PV\":72,\"LOCATION_TYPE\":2,\"AVG_ACCELLERATION\":314,\"MAX_ACCELLERATION\":454,\"EVENT_ANGLE\":{\"$numberLong\":\"-106\"},\"DURATION\":{\"$numberLong\":\"4100\"}}";

        String s2 = "{ \"id\" : 3, \"loc\" :{ \"xs\" : 3, \"dfdf\" : { \"ds\": 434} } }";

        Config config = ConfigFactory.parseString(s1);

        Map<String, ConfigValue> properties = new HashMap<>();

        for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
            //String[] keys = ConfigUtil.splitPath(entry.getKey()).toArray(new String[0]);
            System.out.println("Root key = " + entry.getKey() + " " + entry.getValue().unwrapped());
            properties.put(entry.getKey(),  entry.getValue());

        }

        ;

//        Base64.getEncoder().encodeToString(new ObjectOutputStream(new ByteArrayOutputStream()).writeObject());
//        new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(""))).readObject();

        //properties.put("lof", Arrays.asList(30,40));

//        properties.put("lof.type","Point");
//        properties.put("lof.coordinates","[3,7]");

        //Gson gson = new GsonBuilder().setPrettyPrinting().create();
        //System.out.println(gson.toJson(properties));

        Config config1 = ConfigFactory.parseMap(properties);

        config1 = config1.withoutPath(Consts.locationFieldName);
        config1 = config1.withValue(Consts.locationFieldName + ".type", ConfigValueFactory.fromAnyRef("Point"));
        config1 = config1.withValue(Consts.locationFieldName + ".coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(10,20)));

        config1 = config1.withoutPath("VEHICLE_ID");
        config1 = config1.withValue(Consts.vehicleFieldName, ConfigValueFactory.fromAnyRef(Consts.vehicleFieldName));

        config1 = config1.withValue(Consts.dateFieldName, config1.getValue("TIMESTAMP"));
        config1 = config1.withoutPath("TIMESTAMP");


        System.out.println(config1.root().render(ConfigRenderOptions.concise()));

        System.out.println();
        System.out.println(Document.parse(config1.root().render(ConfigRenderOptions.concise())));



        //System.out.println(gson.toJson(properties));


        /*.root()
                .forEach((b1,b2) -> {
            System.out.println(b1 +" "+b2);
        });*/

        //s.forEach(k-> System.out.println(k));


    }
}