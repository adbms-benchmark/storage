package benchmark.caching;

import benchmark.BenchmarkContext;
import benchmark.BenchmarkQuery;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class CachingBenchmarkContext extends BenchmarkContext {
    
    private static final int REPEAT_QUERIES_ONCE = 1;
    
    protected long cacheSize;
    
    public CachingBenchmarkContext(String dataDir) {
        super(REPEAT_QUERIES_ONCE, dataDir, -1, TYPE_CACHING);
        arrayDimensionality = 3;
        arraySize = CachingBenchmarkDataManager.DATA_SIZE;
        arraySizeShort = CachingBenchmarkDataManager.DATA_SIZE_SHORT;
        updateArrayName();
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }
    
    @Override
    public String getBenchmarkSpecificHeader() {
        return "Query, Cache size, Execution time (ms), ";
    }
    
    @Override
    public String getBenchmarkResultLine(BenchmarkQuery query) {
        return String.format("\"%s\", %s, ", query.getQueryString(), getCacheSize());
    }
    
}
