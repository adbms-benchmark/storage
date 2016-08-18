/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmark;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

/**
 * Entry point class.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public abstract class Driver {

    protected Logger log;
    
    public int runMain(Driver driver, String... args) throws JSAPException, IOException {
        SimpleJSAP jsap = driver.getCmdLineConfig(driver.getClass());
        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }
        driver.setupLogger(config.getBoolean("verbose"), Driver.class);
        return driver.runBenchmark(config);
    }
    
    protected SimpleJSAP getCommonCmdLineConfig(Class c) throws JSAPException {
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
                    new FlaggedOption("tilesize", JSAP.LONG_PARSER, String.valueOf(BenchmarkContext.DEFAULT_TILE_SIZE), JSAP.REQUIRED,
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
                    new Switch("cleanquery", JSAP.NO_SHORTFLAG,
                            "clean-query", "Drop caches and restart systems on every query; by default this is only done on every benchmark session."),
                    new Switch("verbose",
                            'v', "verbose", "Print extra information.")
                }
        );
        return jsap;
    }
    
    protected abstract SimpleJSAP getCmdLineConfig(Class c) throws JSAPException;

    protected String getMainName(Class c) {
        return "run.sh";
    }
    
    protected abstract String getDescription();

    protected void setupLogger(boolean verbose, Class c) {
        if (verbose) {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
        } else {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
        }
        System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
        log = LoggerFactory.getLogger(c);
    }
    
    protected abstract int runBenchmark(JSAPResult config) throws IOException;

    protected int runBenchmark(BenchmarkContext benchmarkContext, AdbmsSystem systemController) throws IOException {
        int exitCode = 0;
        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchmarkContext);
        QueryExecutor queryExecutor = systemController.getQueryExecutor(benchmarkContext);
        DataManager dataManager = systemController.getDataManager(benchmarkContext, queryExecutor);
        BenchmarkExecutor benchmark = new BenchmarkExecutor(benchmarkContext, queryGenerator, queryExecutor, dataManager, systemController);
        try {
            benchmark.runBenchmark();
        } catch (Exception ex) {
            log.error("Failed executing benchmark.", ex);
            exitCode = 1;
        }
        return exitCode;
    }
}
