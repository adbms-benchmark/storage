package benchmark.operations;

import benchmark.BenchmarkContext;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import data.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Danut Rusu on 23.03.17.
 */
public abstract class OperationsBenchmarkDataManager<T> extends DataManager<T> {

    private static final Logger log = LoggerFactory.getLogger(OperationsBenchmarkDataManager.class);

    public static final int CELL_TYPE_SIZE = 8;

    public static final int BAND_WIDTH = 32768;//11312;
    public static final int BAND_HEIGHT = 32768;//11312;
//    public static final long ARRAY_SIZE = BAND_WIDTH * BAND_HEIGHT * CELL_TYPE_SIZE;

    public static final String ARRAY_SIZE_SHORT = "1GB";

    public static final int ARRAY_NO = 2;

    public OperationsBenchmarkDataManager(T systemController, QueryExecutor<T> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public void generateData() throws Exception {
        if (benchmarkContext.isGenerateData()) {
            String fileName = benchmarkContext.getArrayName();
            RandomDataGenerator dataGen = new RandomDataGenerator(benchmarkContext.getArraySize(), benchmarkContext.getDataDir(), fileName);
            String filePath = dataGen.getFilePath();
            log.debug("Generated benchmark data slice: " + filePath);
        }
    }
}
