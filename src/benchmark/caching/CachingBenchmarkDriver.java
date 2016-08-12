package benchmark.caching;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import benchmark.Driver;
import benchmark.AdbmsSystem;
import benchmark.BenchmarkContext;
import java.io.IOException;

/**
 * Entry point for the caching benchmark.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class CachingBenchmarkDriver extends Driver {

    @Override
    protected SimpleJSAP getCmdLineConfig(Class c) throws JSAPException {
        
        SimpleJSAP jsap = new SimpleJSAP(
                getMainName(c),
                getDescription(),
                new Parameter[]{
                    new FlaggedOption("system", JSAP.STRING_PARSER, "rasdaman,scidb", JSAP.REQUIRED,
                            's', "systems", "Array DBMS to target in this run.").setList(true).setListSeparator(','),
                    new FlaggedOption("config", JSAP.STRING_PARSER, "conf/rasdaman.properties,conf/scidb.properties", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "system-configs", "System configuration (connection details, directories, etc).").setList(true).setListSeparator(','),
                    new FlaggedOption("cacheSizes", JSAP.LONG_PARSER, "1073741824,2147483648,3221225472,4294967296,8589934592", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "cache-sizes", "Cache sizes (in bytes) to benchmark.").setList(true).setListSeparator(','),
                    new FlaggedOption("datadir", JSAP.STRING_PARSER, "/tmp", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "datadir", "Data directory, for temporary and permanent data used in ingestion."),
                    new FlaggedOption("tilesize", JSAP.STRING_PARSER, String.valueOf(BenchmarkContext.DEFAULT_TILE_SIZE), JSAP.REQUIRED,
                            't', "tile-size", "Tile size in bytes."),
                    new Switch("load", JSAP.NO_SHORTFLAG,
                            "load", "Load data."),
                    new Switch("drop", JSAP.NO_SHORTFLAG,
                            "drop", "Drop data."),
                    new Switch("generate", JSAP.NO_SHORTFLAG,
                            "generate", "Generate benchmark data."),
                    new Switch("nobenchmark", JSAP.NO_SHORTFLAG,
                            "disable-benchmark", "Do not run benchmark, just create or drop data."),
                    new Switch("norestart", JSAP.NO_SHORTFLAG,
                            "disable-restart", "Do not restart the benchmarked systems."),
                    new Switch("verbose",
                            'v', "verbose", "Print extra information.")
                }
        );
        return jsap;
    }

    @Override
    protected int runBenchmark(JSAPResult config) throws IOException {
        int exitCode = 0;

        CachingBenchmarkContext benchmarkContext = new CachingBenchmarkContext(config.getString("datadir"));
        benchmarkContext.setLoadData(config.getBoolean("load"));
        benchmarkContext.setDropData(config.getBoolean("drop"));
        benchmarkContext.setGenerateData(config.getBoolean("generate"));
        benchmarkContext.setDisableBenchmark(config.getBoolean("nobenchmark"));
        benchmarkContext.setDisableSystemRestart(config.getBoolean("norestart"));

        String[] systems = config.getStringArray("system");
        String[] configs = config.getStringArray("config");
        if (systems.length != configs.length) {
            throw new IllegalArgumentException(systems.length + " systems specified, but " + configs.length + " system configuration files.");
        }
        
        long[] cacheSizes = config.getLongArray("cacheSizes");
        if (config.contains("tilesize")) {
            long tileSize = config.getLong("tilesize");
            benchmarkContext.setTileSize(tileSize);
        }
        
        int configInd = 0;
        for (String system : systems) {
            String configFile = configs[configInd++];
            AdbmsSystem adbmsSystem = AdbmsSystem.getAdbmsSystem(system, configFile, benchmarkContext);
            if (benchmarkContext.isDisableBenchmark()) {
                exitCode += runBenchmark(benchmarkContext, adbmsSystem);
            } else {
                for (long cacheSize : cacheSizes) {
                    benchmarkContext.setCacheSize(cacheSize);
                    adbmsSystem.setSystemCacheSize(cacheSize);
                    log.info("Cache size set to " + cacheSize + " bytes in " + adbmsSystem.getSystemName() + ".");
                    exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                }
            }
        }

        return exitCode;
    }
    
    public static void main(String... args) throws Exception {
        CachingBenchmarkDriver driver = new CachingBenchmarkDriver();
        System.exit(driver.runMain(driver, args));
    }

    @Override
    protected String getDescription() {
        return "Benchmark caching behaviour in Array Databases. Currently supported systems: rasdaman, SciDB.";
    }
}
