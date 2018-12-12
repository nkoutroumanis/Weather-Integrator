package com.github.nkoutroumanis;

import com.mchange.v1.db.sql.ConnectionUtils;
import org.junit.Test;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExportVariablesFromGribFileTest {

    @Test
    public void exportVariablesFromFile000() throws IOException {
        NetcdfFile ncf = NetcdfFile.open("./grib_files/gfs_4_20170608_0000_000.grb2"); //loading grib file
        PrintWriter writer = new PrintWriter("variables/variables-of-000.txt", "UTF-8");
        List<Variable> vars = ncf.getVariables(); //listing all variables
        for (Variable var : vars) {
            writer.println(var.getNameAndDimensions());
            System.out.println();
        }

        writer.close();

    }

    @Test
    public void exportVariablesFromFile003() throws IOException {
        NetcdfFile ncf = NetcdfFile.open("./grib_files/gfs_4_20160415_0000_003.grb2"); //loading grib file
        PrintWriter writer = new PrintWriter("variables/variables-of-003.txt", "UTF-8");
        List<Variable> vars = ncf.getVariables(); //listing all variables
        for (Variable var : vars) {
            writer.println(var.getNameAndDimensions());
            System.out.println();
        }

        writer.close();

    }

    @Test
    public void exportVariablesFromFile006() throws IOException {
        NetcdfFile ncf = NetcdfFile.open("./grib_files/gfs_4_20170608_0000_006.grb2"); //loading grib file
        PrintWriter writer = new PrintWriter("variables/variables-of-006.txt", "UTF-8");
        List<Variable> vars = ncf.getVariables(); //listing all variables
        for (Variable var : vars) {
            writer.println(var.getNameAndDimensions());
            System.out.println();
        }

        writer.close();

    }

    @Test
    public void commonVariablesofFiles0000003006() throws IOException {
        NetcdfFile ncf;
        List<String> vars000 = new ArrayList<>();
        List<String> vars003 = new ArrayList<>();
        List<String> vars006 = new ArrayList<>();

        ncf =  NetcdfFile.open("./grib_files/gfs_4_20170608_0000_000.grb2");
        List<Variable> v = ncf.getVariables(); //listing all variables
        for (Variable var : v) {
            vars000.add(var.getNameAndDimensions());
        }

        ncf =  NetcdfFile.open("./grib_files/gfs_4_20160415_0000_003.grb2");
        v = ncf.getVariables(); //listing all variables
        for (Variable var : v) {
            vars003.add(var.getNameAndDimensions());
        }

        ncf =  NetcdfFile.open("./grib_files/gfs_4_20170608_0000_006.grb2");
        v = ncf.getVariables(); //listing all variables
        for (Variable var : v) {
            vars006.add(var.getNameAndDimensions());
        }

        vars000.stream().filter(vars003::contains).collect(Collectors.toList());


        PrintWriter writer;

        writer = new PrintWriter("variables/commons-000-003.txt", "UTF-8");
        for (String var : vars000.stream().filter(vars003::contains).collect(Collectors.toList())) {
            writer.println(var);
            System.out.println();
        }
        writer.close();

        writer = new PrintWriter("variables/commons-003-006.txt", "UTF-8");
        for (String var : vars003.stream().filter(vars006::contains).collect(Collectors.toList())) {
            writer.println(var);
            System.out.println();
        }
        writer.close();

        writer = new PrintWriter("variables/commons-000-006.txt", "UTF-8");
        for (String var : vars000.stream().filter(vars006::contains).collect(Collectors.toList())) {
            writer.println(var);
            System.out.println();
        }
        writer.close();

        writer = new PrintWriter("variables/commons-000-003-006.txt", "UTF-8");
        for (String var : vars000.stream().filter(vars003::contains).filter(vars006::contains).collect(Collectors.toList())) {
            writer.println(var);
            System.out.println();
        }
        writer.close();

    }


}