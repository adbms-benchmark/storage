package benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.BenchmarkUtil;
import util.IO;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a benchmark, considering all the given ingredients (context, ADBMS
 * system, query generator).
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkExecutor {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkExecutor.class);

    public static final int MAX_RETRY = 3;

    private final QueryGenerator queryGenerator;
    private final QueryExecutor queryExecutor;
    private final AdbmsSystem systemController;
    private final BenchmarkContext benchmarkContext;
    private final DataManager dataManager;

    public BenchmarkExecutor(BenchmarkContext benchmarkContext, QueryGenerator queryGenerator,
            QueryExecutor queryExecutor, DataManager dataManager, AdbmsSystem systemController) {
        this.queryGenerator = queryGenerator;
        this.queryExecutor = queryExecutor;
        this.systemController = systemController;
        this.benchmarkContext = benchmarkContext;
        this.dataManager = dataManager;
    }

    public void runBenchmark() throws Exception {
        log.info("Executing " + benchmarkContext.getBenchmarkType()
                + " benchmark on " + systemController.getSystemName() + ", "
                + benchmarkContext.getArrayDimensionality() + "D data of size "
                + benchmarkContext.getArraySizeShort() + " (" + benchmarkContext.getArraySize() + "B)");

        boolean alreadyDropped = false;

        File resultsDir = IO.getResultsDir();
        String queryName = systemController.getSystemName() + benchmarkContext.getArrayDimensionality() + "D-size" + benchmarkContext.getArraySize() + "B";
        String resultsFilename = systemController.getSystemName() + "_benchmark_results." + System.currentTimeMillis() + "." + queryName + ".csv";
        File resultsFile = new File(resultsDir.getAbsolutePath(), resultsFilename);

        try (PrintWriter pr = new PrintWriter(new FileWriter(resultsFile, true))) {
            loadData(pr);

            runBenchmark(pr);

            alreadyDropped = dropData(pr);
        } finally {
            if (!alreadyDropped) {
                dropData(null);
            }
        }
    }

    private boolean runBenchmark(PrintWriter pr) throws Exception {
        if (benchmarkContext.isDisableBenchmark()) {
            return false;
        }
        
        pr.println("----------------------------------------------------------------------------");
        pr.println("# Starting benchmark");
        pr.println("");

        Benchmark benchmark = queryGenerator.getBenchmark();
        Double msElapsed = 0.0;
        for (BenchmarkSession session : benchmark.getBenchmarkSessions()) {
            msElapsed += runBenchmarkSession(session, pr);
        }

        pr.println("----------------------------------------------------------------------------");
        pr.println("# Total benchmark execution time (ms): " + msElapsed);

        return true;
    }

    private Double runBenchmarkSession(BenchmarkSession session, PrintWriter pr) throws Exception {
        pr.println("----------------------------------------------------------------------------");
        pr.println("# Benchmark session: " + session.getDescription());
        pr.println("System, " + benchmarkContext.getBenchmarkSpecificHeader() + "Mean execution time (ms)");

        systemController.restartSystem();
        BenchmarkUtil.dropSystemCaches();
        
        Double msElapsed = 0.0;
        for (BenchmarkQuery query : session.getBenchmarkQueries()) {
            msElapsed += runBenchmarkQuery(query, pr);
        }
        pr.println("# Benchmark session '" + session.getDescription() + "' execution time (ms): " + msElapsed);
        pr.flush();
        return msElapsed;
    }

    private Double runBenchmarkQuery(BenchmarkQuery query, PrintWriter pr) {
        log.info("Executing benchmark query: " + query.getQueryString());
        
        List<Long> queryExecutionTimes = new ArrayList<>();
        
        int repeatNumber = benchmarkContext.getRepeatNumber();
        for (int repeatIndex = 0; repeatIndex < repeatNumber; ++repeatIndex) {
            boolean failed = true;
            long time = -1;
            for (int retryIndex = 0; retryIndex < MAX_RETRY && failed; ++retryIndex) {
                try {
                    if (benchmarkContext.isCleanQuery()) {
                        systemController.restartSystem();
                        BenchmarkUtil.dropSystemCaches();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    time = queryExecutor.executeTimedQuery(query.getQueryString());
                    log.debug(" -> " + time + "ms");
                    failed = false;
                } catch (Exception ex) {
                    log.warn(" query \"" + query.getQueryString() + "\" failed on try " + (retryIndex + 1) + ". Retrying.");
                }
            }
            queryExecutionTimes.add(time);
        }

        StringBuilder resultLine = new StringBuilder();
        resultLine.append(String.format("%s, %s",
                systemController.getSystemName(), benchmarkContext.getBenchmarkResultLine(query)));
        
        for (Long queryExecutionTime : queryExecutionTimes) {
            resultLine.append(queryExecutionTime);
            resultLine.append(", ");
        }
        Double ret = BenchmarkUtil.getBenchmarkMean(queryExecutionTimes);
        resultLine.append(ret);

        pr.println(resultLine.toString());
        pr.flush();
        return ret;
    }

    private boolean loadData(PrintWriter pr) throws Exception {
        if (benchmarkContext.isLoadData()) {
            systemController.restartSystem();
            if (benchmarkContext.isGenerateData()) {
                log.debug("Generating data...");
                dataManager.generateData();
            }
            long msElapsed = dataManager.loadData();
            if (pr != null) {
                pr.println("Loaded benchmark data in (ms): " + msElapsed);
            } else {
                log.info("Loaded benchmark data in (ms): " + msElapsed);
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean dropData(PrintWriter pr) throws Exception {
        if (benchmarkContext.isDropData()) {
            systemController.restartSystem();
            long msElapsed = dataManager.dropData();
            if (pr != null) {
                pr.println("Dropped benchmark data in (ms): " + msElapsed);
            }
            return true;
        } else {
            return false;
        }
    }

}
