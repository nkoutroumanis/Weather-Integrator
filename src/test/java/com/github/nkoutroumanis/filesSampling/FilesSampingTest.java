package com.github.nkoutroumanis.filesSampling;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class FilesSampingTest {

    @Test
    public void randomNumbers() {
        Random r = new Random();
        long generatedLong = 1 + (long) (Math.random() * (10 - 1));
        System.out.println(generatedLong);
    }


}