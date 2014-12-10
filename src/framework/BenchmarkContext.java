package framework;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContext extends Context {
    
    private final long collSize;
    private final long maxQuerySelectSize;
    private final long collTileSize;
    private final String collName;
    
    public static final String KEY_COLL_SIZE = "coll.size";
    public static final String KEY_COLL_NAME = "coll.name";
    public static final String KEY_COLL_TILE_SIZE = "coll.tile_size";
    public static final String KEY_QUERY_SELECT_SIZE = "query.select_size";

    public BenchmarkContext(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath);
        collSize = getValueLong(KEY_COLL_SIZE);
        collName = getValue(KEY_COLL_NAME);
        maxQuerySelectSize = getValueLong(KEY_QUERY_SELECT_SIZE);
        collTileSize = getValueLong(KEY_COLL_TILE_SIZE);
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

    public String getCollName() {
        return collName;
    }

    @Override
    public String toString() {
        return "Benchmark context:" + "\n collSize=" + collSize + 
                "\n maxQuerySelectSize=" + maxQuerySelectSize + "\n collTileSize=" + collTileSize + "\n collName=" + collName;
    }
    
}
