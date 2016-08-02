/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package driver;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.SimpleJSAP;
import framework.AdbmsSystem;
import framework.BenchmarkExecutor;
import framework.DataManager;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
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
        driver.setupLogger(config.getBoolean("verbose"), StorageBenchmarkDriver.class);
        return driver.runBenchmark(config);
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
