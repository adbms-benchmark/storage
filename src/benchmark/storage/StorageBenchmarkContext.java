package benchmark.storage;

import benchmark.BenchmarkContext;
import benchmark.BenchmarkQuery;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class StorageBenchmarkContext extends BenchmarkContext {
    
    protected final double maxSelectSizePercent;
    protected final int queryNumber;
    
    public StorageBenchmarkContext(double maxSelectSizePercent, long tileSize, int queryNumber, int repeatNumber, String dataDir, int timeout) {
        super(repeatNumber, dataDir, timeout, TYPE_STORAGE);
        this.maxSelectSizePercent = maxSelectSizePercent;
        this.tileSize = tileSize;
        this.queryNumber = queryNumber;
    }
    
    public long getMaxSelectSize() {
        return (long) (((double) arraySize * maxSelectSizePercent) / 100.0);
    }

    public double getMaxSelectSizePercent() {
        return maxSelectSizePercent;
    }

    public int getQueryNumber() {
        return queryNumber;
    }
    
    @Override
    public String getBenchmarkSpecificHeader() {
        String ret = "Query, Query type, Array dimension, Array size, Max select size, ";
        for (int i = 0; i < repeatNumber; i++) {
            ret += "Execution time " + i + " (ms), ";
        }
        return ret;
    }
    
    @Override
    public String getBenchmarkResultLine(BenchmarkQuery query) {
        return String.format("\"%s\", %s, %d, %d, %d, ", 
                query.getQueryString(), query.getQueryType().toString(),
                query.getDimensionality(), getArraySize(), getMaxSelectSize());
    }
}
