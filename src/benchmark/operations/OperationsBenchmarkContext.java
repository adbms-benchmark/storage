package benchmark.operations;

import benchmark.BenchmarkContext;
import benchmark.BenchmarkQuery;

/**
 * Created by Danut Rusu on 23.03.17.
 */
public class OperationsBenchmarkContext extends BenchmarkContext {

    private static final int REPEAT_QUERIES_ONCE = 1;
    protected final double maxSelectSizePercent;
    protected final int queryNumber;

    protected long dataSize;
    protected String dataType;

    public OperationsBenchmarkContext(double maxSelectSizePercent, long tileSize, int queryNumber, int repeatNumber, String dataDir, int timeout) {
        super(repeatNumber, dataDir, timeout, TYPE_OPERATIONS);
        this.maxSelectSizePercent = maxSelectSizePercent;
        this.tileSize = tileSize;
        this.queryNumber = queryNumber;
        cleanQuery = true;
        updateArrayName();

    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public long getDataSize() {
            return dataSize;
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


    public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
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