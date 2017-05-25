/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmark.caching;

import benchmark.BenchmarkContext;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import data.RandomDataGenerator;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 * @param <T> ADBMS system
 */
public abstract class CachingBenchmarkDataManager<T> extends DataManager<T> {

    private static final Logger log = LoggerFactory.getLogger(CachingBenchmarkDataManager.class);
    
    public static final int CELL_TYPE_SIZE = 8;
    
    public static final int BAND_WIDTH = 11312;
    public static final int BAND_HEIGHT = 11312;
    public static final int ARRAY_SIZE = BAND_WIDTH * BAND_HEIGHT * CELL_TYPE_SIZE;
    
    public static final String ARRAY_SIZE_SHORT = "1GB";
    
    public static final int ARRAY_NO = 2;
    
    public CachingBenchmarkDataManager(T systemController, QueryExecutor<T> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public void generateData() throws Exception {
        if (benchmarkContext.isGenerateData()) {
            for (int i = 0; i < ARRAY_NO; i++) {
                String fileName = benchmarkContext.getArrayNameN(i);
                RandomDataGenerator dataGen = new RandomDataGenerator(ARRAY_SIZE, benchmarkContext.getDataDir(), fileName);
                String filePath = dataGen.getFilePath();
                log.debug("Generated benchmark data slice: " + filePath);
            }
        }
    }
    
    protected List<String> getSliceFilePaths(BenchmarkContext benchmarkContext) {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < ARRAY_NO; i++) {
            String fileName = benchmarkContext.getArrayNameN(i);
            String sliceFilePath = IO.concatPaths(benchmarkContext.getDataDir(), fileName);
            if (!IO.fileExists(sliceFilePath)) {
                break;
            }
            ret.add(sliceFilePath);
        }
        return ret;
    }
}
