package framework;

import data.BenchmarkQuery;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DomainUtil;
import util.IO;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class Benchmark {

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);

    public static final int MAX_RETRY = 3;

    private final QueryGenerator queryGenerator;
    private final QueryExecutor queryExecutor;
    private final AdbmsSystem systemController;
    private final BenchmarkContext benchmarkContext;

    public Benchmark(BenchmarkContext benchmarkContext, QueryGenerator queryGenerator,
            QueryExecutor queryExecutor, AdbmsSystem systemController) {
        this.queryGenerator = queryGenerator;
        this.queryExecutor = queryExecutor;
        this.systemController = systemController;
        this.benchmarkContext = benchmarkContext;
    }

    public void runBenchmark() throws Exception {
        log.info("Executing benchmark on " + systemController.getSystemName() + ", "
                + benchmarkContext.getArrayDimensionality() + "D data of size "
                + benchmarkContext.getArraySizeShort() + " (" + benchmarkContext.getArraySize() + "B)");

        File resultsDir = IO.getResultsDir();
        File resultsFile = new File(resultsDir.getAbsolutePath(), systemController.getSystemName() + "_benchmark_results.csv");

        try (PrintWriter pr = new PrintWriter(new FileWriter(resultsFile, true))) {
            // the query executor should check whether a collection is already created
            if (benchmarkContext.isCreateData()) {
                systemController.restartSystem();
                queryExecutor.createCollection();
            }
            if (benchmarkContext.isDisableBenchmark()) {
                return;
            }

            long arraysSize = benchmarkContext.getArraySize();
            long maxSelectSize = benchmarkContext.getMaxSelectSize();

            List<BenchmarkQuery> benchmarkQueries = new ArrayList<>();
            // if collections size is 100MB or 1GB run all classes
            if (arraysSize > DomainUtil.SIZE_100MB) {
                benchmarkQueries.addAll(queryGenerator.getBenchmarkQueries());
            }
            benchmarkQueries.add(queryGenerator.getMiddlePointQuery());

            for (BenchmarkQuery query : benchmarkQueries) {
                log.info("Executing query: " + query.getQueryString());

                List<Long> queryExecutionTimes = new ArrayList<>();
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
                    }
                    resultLine.append("\"");
                    resultLine.append(queryExecutionTime);
                    resultLine.append("\"");
                }

                pr.println(resultLine.toString());
                pr.flush();
            }
        } finally {
            if (benchmarkContext.isDropData()) {
                queryExecutor.dropCollection();
            }
        }
    }

}
