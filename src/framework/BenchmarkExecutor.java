package framework;

import data.Benchmark;
import data.BenchmarkQuery;
import data.BenchmarkSession;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;

/**
 * Runs a benchmark, considering all the given ingredients (context, ADBMS system, 
 * query generator).
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
        log.info("Executing " + benchmarkContext.getBenchmarkType() + 
                " benchmark on " + systemController.getSystemName() + ", "
                + benchmarkContext.getArrayDimensionality() + "D data of size "
                + benchmarkContext.getArraySizeShort() + " (" + benchmarkContext.getArraySize() + "B)");
        
        boolean alreadyDropped = false;

        File resultsDir = IO.getResultsDir();
        File resultsFile = new File(resultsDir.getAbsolutePath(), systemController.getSystemName() + "_benchmark_results.csv");

        try (PrintWriter pr = new PrintWriter(new FileWriter(resultsFile, true))) {
            
            if (benchmarkContext.isCreateData()) {
                systemController.restartSystem();
                long loadDataTime = dataManager.loadData();
                pr.println("Loaded benchmark data in (ms): " + loadDataTime);
            }
            
            if (!benchmarkContext.isDisableBenchmark()) {
                long arraysSize = benchmarkContext.getArraySize();
                long maxSelectSize = benchmarkContext.getMaxSelectSize();

                Benchmark benchmark = queryGenerator.getBenchmark();
                for (BenchmarkSession session : benchmark.getBenchmarkSessions()) {
                    pr.println("# Benchmark session: " + session.getDescription());
                    long totalQueryExecutionTime = 0;
                    for (BenchmarkQuery query : session.getBenchmarkQueries()) {
                        List<Long> queryExecutionTimes = new ArrayList<>();
                        log.info("Executing query: " + query.getQueryString());
                        for (int repeatIndex = 0; repeatIndex < benchmarkContext.getRepeatNumber(); ++repeatIndex) {
                            boolean failed = true;
                            long time = -1;

                            for (int retryIndex = 0; retryIndex < MAX_RETRY && failed; ++retryIndex) {
                                try {
                                    systemController.restartSystem();
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
                        resultLine.append(String.format("\"%s\", \"%s\", \"%s\", \"%d\", \"%d\", \"%d\", ",
                                systemController.getSystemName(), query.getQueryType().toString(), query.getQueryString(),
                                query.getDimensionality(), arraysSize, maxSelectSize));

                        boolean isFirst = true;
                        for (Long queryExecutionTime : queryExecutionTimes) {
                            if (!isFirst) {
                                resultLine.append(", ");
                                isFirst = false;
                            } else {
                                totalQueryExecutionTime += queryExecutionTime;
                            }
                            resultLine.append(queryExecutionTime);
                        }

                        pr.println(resultLine.toString());
                        pr.flush();
                    }

                    pr.println("Benchmark session '" + session.getDescription() + "' execution time (ms): " + totalQueryExecutionTime);
                }
            }
            
            if (benchmarkContext.isDropData()) {
                systemController.restartSystem();
                long dropDataTime = dataManager.dropData();
                pr.println("Dropped benchmark data in (ms): " + dropDataTime);
                alreadyDropped = true;
            }
        } finally {
            if (benchmarkContext.isDropData() && !alreadyDropped) {
                systemController.restartSystem();
                long dropDataTime = dataManager.dropData();
                log.info("Dropped benchmark data in (ms): " + dropDataTime);
            }
        }
    }

}
