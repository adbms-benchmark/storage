/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

/**
 * Benchmark utilities.
 *
 * @author Dimitar Misev
 */
public class BenchmarkUtil {

    public static String getArrayName(int dimension, String size) {
        return "colD" + dimension + "S" + size;
    }

    public static String getAsqldbCollectionNameInRasdaman(String tableName, String columnName) {
        return "PUBLIC_" + tableName.toUpperCase() + "_" + columnName.toUpperCase();
    }
}
