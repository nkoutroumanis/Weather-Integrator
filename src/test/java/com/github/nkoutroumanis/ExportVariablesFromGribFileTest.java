package com.github.nkoutroumanis;

import org.junit.Test;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ExportVariablesFromGribFileTest {

    @Test
    public void exportVariablesInFile() throws IOException {
        NetcdfFile ncf = NetcdfFile.open("./grib_files/gfs_4_20160415_0000_003.grb2"); //loading grib file
        PrintWriter writer = new PrintWriter("variables/all-variables.txt", "UTF-8");
        List<Variable> vars = ncf.getVariables(); //listing all variables
        for (Variable var : vars) {
            writer.println(var.getName());
            System.out.println();
        }

        writer.close();

    }


}