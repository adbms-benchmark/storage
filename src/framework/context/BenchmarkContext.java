package framework.context;

import framework.context.TableContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
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
    
    private final long collSize;
    private final long maxQuerySelectSize;
    private final long collTileSize;
    private final String collName1;
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
        super(propertiesPath);
        collSize = getValueLong(KEY_COLL_SIZE);
        collName1 = getValue(KEY_COLL_NAME1);
        collName2 = getValue(KEY_COLL_NAME2);
        maxQuerySelectSize = getValueLong(KEY_QUERY_SELECT_SIZE);
        collTileSize = getValueLong(KEY_COLL_TILE_SIZE);
        dataDir = getValue(KEY_DATA_DIR);
        maxQueryRerty = getValueInteger(KEY_QUERY_MAX_RETRY);
        maxQueryExecutionTime = getValueInteger(KEY_QUERY_MAX_EXECUTION_TIME);
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
    
    public String getCollName(long fileSize, int noOfDimensions) {
        StringBuilder createArrayQuery = new StringBuilder("");
        createArrayQuery.append(getCollName1()).append("_").append(fileSize).append("B_");
        createArrayQuery.append("_").append(noOfDimensions).append("D");
        return createArrayQuery.toString();
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
     * @return Integer value, representing how many times the query should be retried before
     * discarded.
     */
    public int getMaxQueryRerty() {
        return maxQueryRerty;
    }

    /**
     * Get the maximum query execution time. If a query runs longer than this period, its execution
     * is terminated.
     * @return Integer value representing the maximum query execution time in seconds.
     */
    public int getMaxQueryExecutionTime() {
        return maxQueryExecutionTime;
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
