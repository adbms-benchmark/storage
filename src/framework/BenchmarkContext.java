package framework;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public final class BenchmarkContext {
    
    private static final Properties properties = new Properties();
    
    public static final long INVALID_VALUE = -1;
    
    public static final String KEY_COLL_SIZE = "coll.size";
    public static final String KEY_COLL_NAME = "coll.name";
    public static final String KEY_COLL_TILE_SIZE = "coll.tile_size";
    public static final String KEY_QUERY_SELECT_SIZE = "query.select_size";
    
    public static long COLLECTION_SIZE = 50l * 1024l * 1024l;
    public static long MAX_SELECT_SIZE = 5l * 1024 * 1024l;
    public static long TILE_SIZE = 4l * 1024l * 1024l;
    public static String COLLECTION_NAME = "BENCHMARK_COLLECTION";
    
    public static void loadProperties(String filePath) throws IOException {
        properties.clear();
        properties.load(new FileInputStream(filePath));
        
        COLLECTION_SIZE = getValueLong(KEY_COLL_SIZE);
        COLLECTION_NAME = getValue(KEY_COLL_NAME);
        MAX_SELECT_SIZE = getValueLong(KEY_QUERY_SELECT_SIZE);
        TILE_SIZE = getValueLong(KEY_COLL_TILE_SIZE);
    }
    
    public static String getValue(final String key) {
        return properties.getProperty(key);
    }
    
    public static long getValueLong(final String key) {
        try {
            return Long.parseLong(getValue(key));
        } catch (Exception ex) {
            return INVALID_VALUE;
        }
    }

}
