package gr.ds.unipi.stpin;

import com.typesafe.config.*;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

        String json = "{\"id\":\"1391128\",\"status\":\"1\",\"vechicle_id\":\"1391128\",\"timestamp\":1499686755000,\"_longitudeM\":1.0826473E7,\"gpsQuality\":\"3\",\"_latitudeM\":4.3893407E7,\"record_type\":\"\\\"E\\\"\",\"localTime\":\"true\",\"voucherID\":\"353255\",\"oid\":\"\\\"5ce8953f7e49752dfd6eafb2\\\"\",\"speed\":128.0,\"angle\":184.0,\"longitude\":10.826473,\"latitude\":43.893406999999996,\"pv\":69,\"location_tyoe\":1,\"avg_accelleration\":311,\"max_accelleration\":-422,\"eventAngle\":77,\"duration\":7800,\"roadID\":200142,\"osm_id\":30997459,\"road_lon\":10.826427645806081,\"road_lat\":43.89340853477682,\"distance\":5.027563765961066,\"probability\":0.6272885607574282,\"myspeed\":0.0,\"slow_motion\":true,\"gap\":false,\"traj_id\":0,\"path\":\"\",\"execution_time\":5,\"sampling\":-1,\"entrance_time\":-1.0,\"start_stop\":false,\"end_stop\":false,\"road_speed\":100.0,\"road_type\":\"motorway\",\"fraction\":0.9642553096768399,\"roadsID\":[],\"roadsOSM\":[],\"length\":0.0,\"hspeed\":128.0,\"matchizAz\":182.67917428020888}";

        Config config = ConfigFactory.parseString(json);
        Set<Map.Entry<String, ConfigValue>> jsonSet = config.entrySet();

        String[] headers = new String[jsonSet.size()];
        Object[] fieldValues = new Object[jsonSet.size()];

        int k = 0;

        for (Map.Entry<String, ConfigValue> entry: jsonSet) {
            headers[k] = entry.getKey();
            fieldValues[k] = entry.getValue();
            k++;
        }

        System.out.println(headers.length);




//        String s1 = "{\"_id\":{\"$oid\":\"5ce941147e49752dfd8923b0\"},\"VOUCHER_ID\":448903,\"VEHICLE_ID\":1497033,\"RECORD_TYPE\":\"E\",\"TIMESTAMP\":{\"$date\":1512737792000},\"LOCAL_TIME\":true,\"LATITUDE\":{\"$numberLong\":\"43957152\"},\"LONGITUDE\":{\"$numberLong\":\"11137166\"},\"location\":{\"lon\":11.137165999999999,\"lat\":43.957152},\"SPEED\":28,\"HEADING\":44,\"GPS_QUALITY\":3,\"STATUS\":1,\"PV\":72,\"LOCATION_TYPE\":2,\"AVG_ACCELLERATION\":314,\"MAX_ACCELLERATION\":454,\"EVENT_ANGLE\":{\"$numberLong\":\"-106\"},\"DURATION\":{\"$numberLong\":\"4100\"}}";
//
//        String s2 = "{ \"id\" : 3, \"loc\" :{ \"xs\" : 3, \"dfdf\" : { \"ds\": 434} } }";
//
//        Config config = ConfigFactory.parseString(s1);
//
//        Map<String, ConfigValue> properties = new HashMap<>();
//
//        for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
//            //String[] keys = ConfigUtil.splitPath(entry.getKey()).toArray(new String[0]);
//            System.out.println("Root key = " + entry.getKey() + " " + entry.getValue().unwrapped());
//            properties.put(entry.getKey(),  entry.getValue());
//
//        }


//        Base64.getEncoder().encodeToString(new ObjectOutputStream(new ByteArrayOutputStream()).writeObject());
//        new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(""))).readObject();

        //properties.put("lof", Arrays.asList(30,40));

//        properties.put("lof.type","Point");
//        properties.put("lof.coordinates","[3,7]");

        //Gson gson = new GsonBuilder().setPrettyPrinting().create();
        //System.out.println(gson.toJson(properties));

//        Config config1 = ConfigFactory.parseMap(properties);
//
//        config1 = config1.withoutPath(Consts.locationFieldName);
//        config1 = config1.withValue(Consts.locationFieldName + ".type", ConfigValueFactory.fromAnyRef("Point"));
//        config1 = config1.withValue(Consts.locationFieldName + ".coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(10,20)));
//
//        config1 = config1.withoutPath("VEHICLE_ID");
//        config1 = config1.withValue(Consts.vehicleFieldName, ConfigValueFactory.fromAnyRef(Consts.vehicleFieldName));
//
//        config1 = config1.withValue(Consts.dateFieldName, config1.getValue("TIMESTAMP"));
//        config1 = config1.withoutPath("TIMESTAMP");
//
//
//        System.out.println(config1.root().render(ConfigRenderOptions.concise()));
//
//        System.out.println();
//        System.out.println(Document.parse(config1.root().render(ConfigRenderOptions.concise())));



        //System.out.println(gson.toJson(properties));


        /*.root()
                .forEach((b1,b2) -> {
            System.out.println(b1 +" "+b2);
        });*/

        //s.forEach(k-> System.out.println(k));


    }
}