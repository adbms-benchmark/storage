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
    
    public static final int MAX_SLICE_NO = 101;
    public static final int BAND_NO = 11;
    public static final int BAND_WIDTH = 8000;
    public static final int BAND_HEIGHT = 8000;
    
    public static final long SLICE_SIZE = 8000 * 8000 * 11 * 2;
    public static final long DATA_SIZE = SLICE_SIZE * MAX_SLICE_NO;
    public static final String DATA_SIZE_SHORT = "133GB";
    
    public static final String SLICE_EXT = ".bin";
    
    public CachingBenchmarkDataManager(T systemController, QueryExecutor<T> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public void generateData() throws Exception {
        if (benchmarkContext.isGenerateData()) {
            for (int i = 0; i <= MAX_SLICE_NO; i++) {
                String fileName = i + SLICE_EXT;
                RandomDataGenerator dataGen = new RandomDataGenerator(SLICE_SIZE, fileName);
                String filePath = dataGen.getFilePath();
                log.debug("Generated benchmark data slice: " + filePath);
            }
        }
    }
    
    protected List<String> getSliceFilePaths(BenchmarkContext benchmarkContext) {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i <= MAX_SLICE_NO; i++) {
            String sliceFileName = i + SLICE_EXT;
            String sliceFilePath = IO.concatPaths(benchmarkContext.getDataDir(), sliceFileName);
            if (!IO.fileExists(sliceFilePath)) {
                break;
            }
            ret.add(sliceFilePath);
        }
        return ret;
    }
}
