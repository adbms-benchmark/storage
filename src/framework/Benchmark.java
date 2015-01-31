package framework;

import data.BenchmarkQuery;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import util.IO;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class Benchmark {

    public static final int REPEAT_NO = 5;
    public static final int MAX_RETRY = 3;

    private final QueryGenerator queryGenerator;
    private final QueryExecutor queryExecutor;
    private final SystemController systemController;

    public Benchmark(QueryGenerator queryGenerator, QueryExecutor queryExecutor, SystemController systemController) {
        this.queryGenerator = queryGenerator;
        this.queryExecutor = queryExecutor;
        this.systemController = systemController;
    }

    public void runBenchmark(long collectionSize, long maxSelectSize) throws Exception {
        File resultsDir = IO.getResultsDir();
        File resultsFile = new File(resultsDir.getAbsolutePath(), systemController.getSystemName() + "_benchmark_results.csv");
        try (PrintWriter pr = new PrintWriter(new FileWriter(resultsFile, true))) {
            // the query executor should check whether a collection is already created
            if (queryGenerator.noOfDimensions == 1 && collectionSize == 1073741824l) {
                ;
            } else {
                systemController.restartSystem();
                queryExecutor.createCollection();
                systemController.restartSystem();
            }

            List<BenchmarkQuery> benchmarkQueries = new ArrayList<>();

            //if collections size is 100MB or 1GB run all classes
            if (collectionSize == 1073741824l || collectionSize == 104857600l) {
                benchmarkQueries.addAll(queryGenerator.getBenchmarkQueries());
            }

            BenchmarkQuery pointQuery = queryGenerator.getMiddlePointQuery();
            benchmarkQueries.add(pointQuery);

            for (BenchmarkQuery query : benchmarkQueries) {

                System.out.printf("Executing query: \"%s\"\n", query.getQueryString());

                List<Long> queryExecutionTimes = new ArrayList<>();
                for (int repeatIndex = 0; repeatIndex < REPEAT_NO; ++repeatIndex) {
                    boolean failed = true;
                    long time = -1;

                    for (int retryIndex = 0; retryIndex < MAX_RETRY && failed; ++retryIndex) {
                        try {
//                            systemController.restartSystem();
                            time = queryExecutor.executeTimedQuery(query.getQueryString());
                            System.out.print(".. " + time + "ms");
                            failed = false;
                        } catch (Exception ex) {
                            System.out.printf("Query \"%s\" failed on try %d. Retrying...\n", query.getQueryString(), retryIndex + 1);
                        }
                    }
                    queryExecutionTimes.add(time);
                }

                StringBuilder resultLine = new StringBuilder();
                resultLine.append(String.format("\"%s\", \"%s\", \"%s\", \"%d\", \"%d\", \"%d\", ", systemController.getSystemName(), query.getQueryType().toString(), query.getQueryString(), query.getDimensionality(), collectionSize, maxSelectSize));
                boolean isFirst = true;

                for (Long queryExecutionTime : queryExecutionTimes) {
                    if (!isFirst) {
                        resultLine.append(", ");
                    }
                    resultLine.append("\"");
                    resultLine.append(queryExecutionTime);
                    resultLine.append("\"");
                    isFirst = false;
                }

                pr.println(resultLine.toString());
                pr.flush();
                System.out.println("");
            }
        } finally {
//            queryExecutor.dropCollection();
        }
    }

}
