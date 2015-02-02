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
        return "benchmark_" + dimension + "d_" + size.toLowerCase();
    }
}
