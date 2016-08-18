package benchmark;

import util.BenchmarkUtil;

/**
 * Generic benchmark parameters.
 * 
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContext {

    public static final String TYPE_STORAGE = "storage";
    public static final String TYPE_SQLMDA = "sql/mda";
    public static final String TYPE_CACHING = "caching";
    
    public static final long DEFAULT_TILE_SIZE = 16000000;

    protected String arrayName;
    protected long arraySize;
    protected String arraySizeShort;
    protected int arrayDimensionality;
    protected long tileSize;

    protected int repeatNumber;
    protected String dataDir;
    protected int timeout;
    
    protected boolean cleanQuery;
    protected boolean disableSystemRestart;
    protected boolean disableBenchmark;
    protected boolean loadData;
    protected boolean dropData;
    protected boolean generateData;
    protected String benchmarkType;

    protected BenchmarkContext(int repeatNumber, String dataDir, int timeout, String benchmarkType) {
        this.repeatNumber = repeatNumber;
        this.dataDir = dataDir;
        this.timeout = timeout;
        this.benchmarkType = benchmarkType;
        this.tileSize = DEFAULT_TILE_SIZE;
    }

    public String getArrayName() {
        return arrayName;
    }

    public long getArraySize() {
        return arraySize;
    }

    public int getArrayDimensionality() {
        return arrayDimensionality;
    }

    public int getRepeatNumber() {
        return repeatNumber;
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getArraySizeShort() {
        return arraySizeShort;
    }

    public void setArraySizeShort(String arraySizeShort) {
        this.arraySizeShort = arraySizeShort;
    }

    public void setArrayName(String arrayName) {
        this.arrayName = arrayName;
    }

    public void updateArrayName() {
        this.arrayName = BenchmarkUtil.getArrayName(arrayDimensionality, arraySizeShort);
    }

    public void setArraySize(long arraySize) {
        this.arraySize = arraySize;
    }

    public void setArrayDimensionality(int arrayDimensionality) {
        this.arrayDimensionality = arrayDimensionality;
    }

    public int getTimeout() {
        return timeout;
    }

    public long getTileSize() {
        return tileSize;
    }

    public void setTileSize(long tileSize) {
        this.tileSize = tileSize;
    }

    public boolean isDisableBenchmark() {
        return disableBenchmark;
    }

    public void setDisableBenchmark(boolean disableBenchmark) {
        this.disableBenchmark = disableBenchmark;
    }

    public boolean isLoadData() {
        return loadData;
    }

    public void setLoadData(boolean loadData) {
        this.loadData = loadData;
    }

    public boolean isDropData() {
        return dropData;
    }

    public void setDropData(boolean dropData) {
        this.dropData = dropData;
    }

    public boolean isGenerateData() {
        return generateData;
    }

    public void setGenerateData(boolean generateData) {
        this.generateData = generateData;
    }
    
    public String getBenchmarkType() {
        return benchmarkType;
    }

    public void setBenchmarkType(String benchmarkType) {
        this.benchmarkType = benchmarkType;
    }

    public boolean isStorageBenchmark() {
        return TYPE_STORAGE.equalsIgnoreCase(benchmarkType);
    }

    public boolean isSqlMdaBenchmark() {
        return TYPE_SQLMDA.equalsIgnoreCase(benchmarkType);
    }

    public boolean isCachingBenchmark() {
        return TYPE_CACHING.equalsIgnoreCase(benchmarkType);
    }
    
    /**
     * Subclasses should override this method in order to get proper csv header 
     * in the results file for each benchmarked system.
     * 
     * @return benchmark header specific for this context (should end with a comma).
     */
    public String getBenchmarkSpecificHeader() {
        return "";
    }
    
    /**
     * Subclasses should override this method in order to get proper csv header 
     * in the results file for each benchmarked system.
     * 
     * @param query benchmark query
     * @return result line for the evaluation of the given query (should end with a comma).
     */
    public String getBenchmarkResultLine(BenchmarkQuery query) {
        return "";
    }

    public boolean isCleanQuery() {
        return cleanQuery;
    }

    public void setCleanQuery(boolean cleanQuery) {
        this.cleanQuery = cleanQuery;
    }

    @Override
    public BenchmarkContext clone() {
        BenchmarkContext ret = new BenchmarkContext(repeatNumber, dataDir, timeout, benchmarkType);
        ret.setArrayDimensionality(arrayDimensionality);
        ret.setArrayName(arrayName);
        ret.setArraySize(arraySize);
        ret.setArraySizeShort(arraySizeShort);
        ret.setBenchmarkType(benchmarkType);
        ret.setLoadData(loadData);
        ret.setDisableBenchmark(disableBenchmark);
        ret.setDropData(dropData);
        ret.setCleanQuery(cleanQuery);
        return ret;
    }
    
    public String getArrayName0() {
        return getArrayNameN(0);
    }
    
    public String getArrayName1() {
        return getArrayNameN(1);
    }
    
    public String getArrayName2() {
        return getArrayNameN(2);
    }
    
    public String getArrayNameN(int n) {
        return arrayName + "_" + n;
    }

    public boolean isDisableSystemRestart() {
        return disableSystemRestart;
    }

    public void setDisableSystemRestart(boolean disableSystemRestart) {
        this.disableSystemRestart = disableSystemRestart;
    }

    @Override
    public String toString() {
        return "BenchmarkContext{" + "arrayName=" + arrayName + ", arraySize=" + 
                arraySize + ", arraySizeShort=" + arraySizeShort + ", arrayDimensionality=" + 
                arrayDimensionality + ", repeatNumber=" + repeatNumber + ", dataDir=" + 
                dataDir + ", timeout=" + timeout + ", disableBenchmark=" + 
                disableBenchmark + ", loadData=" + loadData + ", dropData=" + 
                dropData + ", generateData=" + generateData + ", benchmarkType=" + 
                benchmarkType + '}';
    }
}
