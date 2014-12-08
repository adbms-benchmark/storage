package framework;

/**
 *
 * @author George Merticariu
 */
public final class Configuration {
    private Configuration() {

    }

    //TODO-GM: put this into configuration file
    public static final long COLLECTION_SIZE = 50l * 1024l * 1024l;
    public static final long MAX_SELECT_SIZE = 5l * 1024 * 1024l;
    public static final long TILE_SIZE = 4l * 1024l * 1024l;
    public static final String COLLECTION_NAME = "BENCHMARK_COLLECTION";


}
