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
        SimpleJSAP jsap = getCommonCmdLineConfig(c);
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
        benchmarkContext.setCleanQuery(config.getBoolean("cleanquery"));

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
