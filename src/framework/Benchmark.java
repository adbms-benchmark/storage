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
            try {
                queryExecutor.dropCollection();
            } catch (Exception ex) {}
            queryExecutor.createCollection();

            List<String> benchmarkQueries = queryGenerator.getBenchmarkQueries();
            for (String query : benchmarkQueries) {
                for (int i = 0; i < 5; ++i) {
                    systemController.restartSystem();
                    //TODO-GM: add more information about the query (no of dimensions)
                    pr.println(String.format("\"%s\", \"%s\", \"%d\"", systemController.getSystemName(), query, queryExecutor.executeTimedQuery(query)));
                }
                pr.flush();
            }
        } finally {
            queryExecutor.dropCollection();
        }
    }

}
