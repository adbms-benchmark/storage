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
        //TODO-GM: read results file path from config file
        try (PrintWriter pr = new PrintWriter(new FileWriter(HOME_DIR + "/results.csv", true))) {
            systemController.restartSystem();
//            queryExecutor.createCollection();

            List<String> benchmarkQueries = queryGenerator.getBenchmarkQueries();

            for (String query : benchmarkQueries) {
                System.out.printf("Executing query: \"%s\"\n", query);

                pr.print(String.format("\"%s\", \"%s\", \"%d\", \"%d\", \"%d\", ", systemController.getSystemName(), query, noOfDim, collectionSize, maxSelectSize));
                long total = 0;
                int repeatNo = 0;
                for (int repeatIndex = 0; repeatIndex < REPEAT_NO; ++repeatIndex) {
                    //TODO-GM: add more information about the query (no of dimensions)
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
                System.out.printf("Executed in %d ms.\n\n", (total/repeatNo));

                pr.println(total / repeatNo);
                pr.flush();
            }
        } finally {
//            queryExecutor.dropCollection();
        }
    }

}
