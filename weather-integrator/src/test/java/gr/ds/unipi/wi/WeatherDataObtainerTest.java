package gr.ds.unipi.wi;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class WeatherDataObtainerTest {

    @Test
    public void stirngTest() throws URISyntaxException {
//        String s = "hdfs://localhost:50070/dfioijfd/sidfjosidf/fsodjfoio/osijdf/dkfkmmk";
//
//        System.out.println(s.substring(7).substring(0, s.substring(7).indexOf("/") + 1));
//
//        System.out.println(s.substring(7).substring(s.substring(7).indexOf("/")));

        URI uri = new URI("hdfs://localhost:50070/user/root/koutroumanis/bjkbhkjh/sfg");


        System.out.println("hdfs://" + uri.getAuthority() + "/");
        System.out.println(uri.getPath());


    }
}