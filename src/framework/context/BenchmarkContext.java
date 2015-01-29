package framework.context;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContext extends Context {

    public static final TableContext[] dataSizes = new TableContext[]{
            new TableContext(0, 9),
            new TableContext(1, 21),
            new TableContext(2, 56),
            new TableContext(3, 110),
            new TableContext(4, 230),
            new TableContext(5, 500)
    };

    private long collSize;
    private long maxQuerySelectSize;
    private final long collTileSize;
    private String collName1;
    private final String collName2;
    private final String dataDir;

    private final int maxQueryRerty;
    private final int maxQueryExecutionTime;

    public static final String KEY_COLL_SIZE = "coll.size";
    public static final String KEY_COLL_NAME1 = "coll.name1";
    public static final String KEY_COLL_NAME2 = "coll.name2";
    public static final String KEY_COLL_TILE_SIZE = "coll.tile_size";
    public static final String KEY_QUERY_SELECT_SIZE = "query.select_size";
    public static final String KEY_QUERY_MAX_RETRY = "query.max.retry";
    public static final String KEY_QUERY_MAX_EXECUTION_TIME = "query.max.execution_time";
    public static final String KEY_DATA_DIR = "data.dir";

    public BenchmarkContext(String propertiesPath) throws FileNotFoundException, IOException {
//        super(propertiesPath);
//        collSize = getValueLong(KEY_COLL_SIZE);
//        collName1 = getValue(KEY_COLL_NAME1);
//        collName2 = getValue(KEY_COLL_NAME2);
//        maxQuerySelectSize = getValueLong(KEY_QUERY_SELECT_SIZE);
//        collTileSize = getValueLong(KEY_COLL_TILE_SIZE);
//        dataDir = getValue(KEY_DATA_DIR);
//        maxQueryRerty = getValueInteger(KEY_QUERY_MAX_RETRY);
//        maxQueryExecutionTime = getValueInteger(KEY_QUERY_MAX_EXECUTION_TIME);
        collSize = 1024l * 1024l * 1024l;
        collName1 = "gogu";
        collName2 = "gogu";
        maxQuerySelectSize = collSize / 10;
        collTileSize = 4l * 1024l * 1024l;
        dataDir = "";
        maxQueryRerty = 1;
        maxQueryExecutionTime = 1;
    }

    public long getCollSize() {
        return collSize;
    }

    public long getMaxQuerySelectSize() {
        return maxQuerySelectSize;
    }

    public long getCollTileSize() {
        return collTileSize;
    }

    public String getCollName1() {
        return collName1;
    }

    public String getCollName2() {
        return collName2;
    }

    public String getDataDir() {
        return dataDir;
    }

    /**
     * Gets the maximum query retry. If the query execution fails, it will be retried.
     *
     * @return Integer value, representing how many times the query should be retried before
     * discarded.
     */
    public int getMaxQueryRerty() {
        return maxQueryRerty;
    }

    /**
     * Get the maximum query execution time. If a query runs longer than this period, its execution
     * is terminated.
     *
     * @return Integer value representing the maximum query execution time in seconds.
     */
    public int getMaxQueryExecutionTime() {
        return maxQueryExecutionTime;
    }

    public void setMaxQuerySelectSize(long maxQuerySelectSize) {
        this.maxQuerySelectSize = maxQuerySelectSize;
    }

    public void setCollName1(String collName1) {
        this.collName1 = collName1;
    }

    public void setCollSize(long collSize) {
        this.collSize = collSize;
    }

    @Override
    public String toString() {
        return "Benchmark context:"
                + "\n collName=" + collName1
                + "\n collSize=" + collSize
                + "\n dataFile=" + dataDir
                + "\n maxQuerySelectSize=" + maxQuerySelectSize
                + "\n collTileSize=" + collTileSize
                + "\n maxQueryRetry=" + maxQueryRerty
                + "\n maxQueryExecutionTime=" + maxQueryExecutionTime;
    }

}
