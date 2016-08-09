/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.Collections;
import java.util.List;

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
    
    public static Double getBenchmarkMean(List<Long> executionTimes) {
        if (executionTimes.size() < 2) {
            return getMean(executionTimes);
        } else {
            Collections.sort(executionTimes);
            executionTimes.remove(0);
            executionTimes.remove(executionTimes.size() - 1);
            return getMean(executionTimes);
        }
    }
    
    public static Double getMean(List<Long> list) {
        if (list.isEmpty()) {
            return 0.0;
        }
        Double ret = 0.0;
        for (Long v : list) {
            ret += v;
        }
        ret /= list.size();
        return ret;
    }
}
