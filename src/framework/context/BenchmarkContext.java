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

    private final int maxSelectSize;
    private final long collTileSize;
    private final int queryNumber;
    private final int retryNumber;
    private final String dataDir;

    public BenchmarkContext(int maxSelectSize, long collTileSize, int queryNumber, int retryNumber, String dataDir) {
        this.maxSelectSize = maxSelectSize;
        this.collTileSize = collTileSize;
        this.queryNumber = queryNumber;
        this.retryNumber = retryNumber;
        this.dataDir = dataDir;
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

    public int getMaxSelectSize() {
        return maxSelectSize;
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

    @Override
    public String toString() {
        return "BenchmarkContext:\n"
                + " arrayName=" + arrayName
                + "\n arraySize=" + arraySize
                + "\n arraySizeShort=" + arraySizeShort
                + "\n arrayDimensionality=" + arrayDimensionality
                + "\n maxSelectSize=" + maxSelectSize
                + "\n collTileSize=" + collTileSize
                + "\n queryNumber=" + queryNumber
                + "\n retryNumber=" + retryNumber
                + "\n dataDir=" + dataDir;
    }

}
