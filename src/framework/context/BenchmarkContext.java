package framework.context;

import util.BenchmarkUtil;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContext {

    public static final String TYPE_STORAGE = "storage";
    public static final String TYPE_SQLMDA = "sql/mda";

    private String arrayName;
    private long arraySize;
    private String arraySizeShort;
    private int arrayDimensionality;

    private final double maxSelectSizePercent;
    private final long collTileSize;
    private final int queryNumber;
    private final int repeatNumber;
    private final String dataDir;
    private final int timeout;
    
    private boolean disableBenchmark;
    private boolean createData;
    private boolean dropData;
    private String benchmarkType;

    private String baseType;
    private BenchmarkContext join;

    public BenchmarkContext(double maxSelectSizePercent, long collTileSize, int queryNumber, int repeatNumber, String dataDir, int timeout) {
        this.maxSelectSizePercent = maxSelectSizePercent;
        this.collTileSize = collTileSize;
        this.queryNumber = queryNumber;
        this.repeatNumber = repeatNumber;
        this.dataDir = dataDir;
        this.timeout = timeout;
        this.benchmarkType = TYPE_STORAGE;
        this.join = null;
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

    public long getMaxSelectSize() {
        return (long) (((double) arraySize * maxSelectSizePercent) / 100.0);
    }

    public double getMaxSelectSizePercent() {
        return maxSelectSizePercent;
    }

    public long getCollTileSize() {
        return collTileSize;
    }

    public int getQueryNumber() {
        return queryNumber;
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

    public boolean isDisableBenchmark() {
        return disableBenchmark;
    }

    public void setDisableBenchmark(boolean disableBenchmark) {
        this.disableBenchmark = disableBenchmark;
    }

    public boolean isCreateData() {
        return createData;
    }

    public void setCreateData(boolean createData) {
        this.createData = createData;
    }

    public boolean isDropData() {
        return dropData;
    }

    public void setDropData(boolean dropData) {
        this.dropData = dropData;
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

    public String getBaseType() {
        return baseType;
    }

    public void setBaseType(String baseType) {
        this.baseType = baseType;
    }

    public BenchmarkContext getJoin() {
        return join;
    }

    public void setJoin(BenchmarkContext join) {
        this.join = join;
    }

    @Override
    public BenchmarkContext clone() {
        BenchmarkContext ret = new BenchmarkContext(maxSelectSizePercent, collTileSize, queryNumber, repeatNumber, dataDir, timeout);
        ret.setArrayDimensionality(arrayDimensionality);
        ret.setArrayName(arrayName);
        ret.setArraySize(arraySize);
        ret.setArraySizeShort(arraySizeShort);
        ret.setBenchmarkType(benchmarkType);
        ret.setCreateData(createData);
        ret.setDisableBenchmark(disableBenchmark);
        ret.setDropData(dropData);
        ret.setBaseType(baseType);
        ret.setJoin(join);
        return ret;
    }

    @Override
    public String toString() {
        return "BenchmarkContext:\n"
                + " arrayName=" + arrayName
                + "\n arraySize=" + arraySize
                + "\n arraySizeShort=" + arraySizeShort
                + "\n arrayDimensionality=" + arrayDimensionality
                + "\n maxSelectSize=" + maxSelectSizePercent
                + "\n collTileSize=" + collTileSize
                + "\n queryNumber=" + queryNumber
                + "\n retryNumber=" + repeatNumber
                + "\n dataDir=" + dataDir;
    }

}
