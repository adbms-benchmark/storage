package driver;

import framework.Benchmark;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.asqldb.AsqldbQueryExecutor;
import framework.asqldb.AsqldbQueryGenerator;
import framework.asqldb.AsqldbSystemController;
import org.asqldb.util.AsqldbConnection;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbSciQL {

    public static void main(String... args) throws Exception {

        // @TODO: take paths from args
        ConnectionContext asqldbContext = new ConnectionContext("conf/asqldb.properties");
        ConnectionContext sciqlContext = new ConnectionContext("conf/sciql.properties");
        BenchmarkContext benchContext = new BenchmarkContext("conf/benchmark.properties");
        AsqldbSystemController sysController = new AsqldbSystemController("conf/system.properties");
        int noQueries = 3;

        for (int noOfDim = 1; noOfDim <= 3; ++noOfDim) {
            try {
                System.out.println("ASQLDB: " + noOfDim + "D");
                {
                    AsqldbQueryExecutor queryExecutor = new AsqldbQueryExecutor(asqldbContext, sysController, benchContext, noOfDim);
                    AsqldbQueryGenerator queryGenerator = new AsqldbQueryGenerator(benchContext, noOfDim, noQueries);

                    Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, sysController);
                    benchmark.runBenchmark();
                }
            } finally {
                AsqldbConnection.close();
            }
        }

    }
}
