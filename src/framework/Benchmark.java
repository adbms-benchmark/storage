package framework;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class Benchmark {

    public static final String HOME_DIR = System.getenv("HOME");
    public static final int REPEAT_NO = 2;
    public static final int MAX_RETRY = 3;

    private final QueryGenerator queryGenerator;
    private final QueryExecutor queryExecutor;
    private final SystemController systemController;

    public Benchmark(QueryGenerator queryGenerator, QueryExecutor queryExecutor, SystemController systemController) {
        this.queryGenerator = queryGenerator;
        this.queryExecutor = queryExecutor;
        this.systemController = systemController;
    }

    public void runBenchmark(int noOfDim, long collectionSize, long maxSelectSize) throws Exception {
        String fileName = HOME_DIR + "/" + systemController.getSystemName() + "_benchmark_results.csv";
        try (PrintWriter pr = new PrintWriter(new FileWriter(fileName, true))) {
            systemController.restartSystem();
            // the query executor should check whether a collection is already created
            queryExecutor.createCollection();

            List<String> benchmarkQueries = queryGenerator.getBenchmarkQueries();
            pr.println("System name, Query, Number of dimensions, Collection size, Maximum selection size, Execution time");

            for (String query : benchmarkQueries) {
                System.out.printf("Executing query: \"%s\"\n", query);

                pr.print(String.format("%s, \"%s\", %d, %d, %d, ", systemController.getSystemName(), query, noOfDim, collectionSize, maxSelectSize));
                long total = 0;
                int repeatNo = 0;
                for (int repeatIndex = 0; repeatIndex < REPEAT_NO; ++repeatIndex) {
                    boolean failed = true;
                    long time = -1;

                    for (int retryIndex = 0; retryIndex < MAX_RETRY && failed; ++retryIndex) {
                        try {
                            systemController.restartSystem();
                            time = queryExecutor.executeTimedQuery(query);
                            failed = false;
                        } catch (Exception ex) {
                            System.out.printf("Query \"%s\" failed. Retrying...\n", query, retryIndex + 1);
                        }
                    }

                    if (!failed) {
                        total += time;
                        repeatNo++;
                        pr.print(time + ", ");
                    } else {
                        System.out.printf("Query \"%s\" failed. Skipping...\n", query);
                    }
                }
                if (repeatNo == 0) {
                    ++repeatNo;
                }
                long avg = total / repeatNo;
                System.out.printf("Executed in %d ms.\n\n", avg);

                pr.println(avg);
                pr.flush();
            }
        } finally {
//            queryExecutor.dropCollection();
        }
    }

}
