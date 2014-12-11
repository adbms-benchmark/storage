package framework;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class Benchmark {

    public static final String HOME_DIR = System.getenv("HOME");
    public static final int REPEAT_NO = 2;

    private final QueryGenerator queryGenerator;
    private final QueryExecutor queryExecutor;
    private final SystemController systemController;

    public Benchmark(QueryGenerator queryGenerator, QueryExecutor queryExecutor, SystemController systemController) {
        this.queryGenerator = queryGenerator;
        this.queryExecutor = queryExecutor;
        this.systemController = systemController;
    }

    public void runBenchmark() throws Exception {
        //TODO-GM: read results file path from config file
        try (PrintWriter pr = new PrintWriter(new FileWriter(HOME_DIR + "/results.csv", true))) {
            systemController.restartSystem();
            queryExecutor.createCollection();

            List<String> benchmarkQueries = queryGenerator.getBenchmarkQueries();
            for (String query : benchmarkQueries) {
                pr.print(String.format("\"%s\", \"%s\", ", systemController.getSystemName(), query));
                long total = 0;
                for (int i = 0; i < REPEAT_NO; ++i) {
                    systemController.restartSystem();
                    //TODO-GM: add more information about the query (no of dimensions)
                    long time = queryExecutor.executeTimedQuery(query);
                    total += time;
                    pr.print(time + ", ");
                }
                pr.println(total / REPEAT_NO);
                pr.flush();
            }
        } finally {
            queryExecutor.dropCollection();
        }
    }

}
