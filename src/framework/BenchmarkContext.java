package framework;

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
    
    public static final String KEY_COLL_SIZE = "coll.size";
    public static final String KEY_COLL_NAME1 = "coll.name1";
    public static final String KEY_COLL_NAME2 = "coll.name2";
    public static final String KEY_COLL_TILE_SIZE = "coll.tile_size";
    public static final String KEY_QUERY_SELECT_SIZE = "query.select_size";
    public static final String KEY_DATA_DIR = "data.dir";

    public BenchmarkContext(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath);
        collSize = getValueLong(KEY_COLL_SIZE);
        collName1 = getValue(KEY_COLL_NAME1);
        collName2 = getValue(KEY_COLL_NAME2);
        maxQuerySelectSize = getValueLong(KEY_QUERY_SELECT_SIZE);
        collTileSize = getValueLong(KEY_COLL_TILE_SIZE);
        dataDir = getValue(KEY_DATA_DIR);
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

    @Override
    public String toString() {
        return "Benchmark context:"
                + "\n collName=" + collName1
                + "\n collSize=" + collSize
                + "\n dataFile=" + dataDir
                + "\n maxQuerySelectSize=" + maxQuerySelectSize
                + "\n collTileSize=" + collTileSize;
    }
    
}
