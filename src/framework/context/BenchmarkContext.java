package framework.context;

import util.BenchmarkUtil;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContext {

    private String arrayName;
    private long arraySize;
    private String arraySizeShort;
    private int arrayDimensionality;

    private final double maxSelectSizePercent;
    private final long collTileSize;
    private final int queryNumber;
    private final int retryNumber;
    private final String dataDir;
    private final int timeout;
    
    private boolean disableBenchmark;
    private boolean createData;
    private boolean dropData;

    public BenchmarkContext(double maxSelectSizePercent, long collTileSize, int queryNumber, int repeatNumber, String dataDir, int timeout) {
        this.maxSelectSizePercent = maxSelectSizePercent;
        this.collTileSize = collTileSize;
        this.queryNumber = queryNumber;
        this.retryNumber = repeatNumber;
        this.dataDir = dataDir;
        this.timeout = timeout;
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

    public int getRetryNumber() {
        return retryNumber;
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
                + "\n retryNumber=" + retryNumber
                + "\n dataDir=" + dataDir;
    }

}
