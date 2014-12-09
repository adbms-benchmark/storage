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

        ConnectionContext asqldbContext = new ConnectionContext("conf/asqldb.properties");
        ConnectionContext sciqlContext = new ConnectionContext("conf/sciql.properties");
        BenchmarkContext benchContext = new BenchmarkContext("conf/benchmark.properties");
        int noQueries = 10;

        for (int noOfDim = 1; noOfDim <= 3; ++noOfDim) {
            try {
                System.out.println("ASQLDB: " + noOfDim + "D");
                {
                    AsqldbSystemController s = new AsqldbSystemController();
                    AsqldbQueryExecutor r = new AsqldbQueryExecutor(asqldbContext, s, benchContext, noOfDim);
                    AsqldbQueryGenerator q = new AsqldbQueryGenerator(benchContext, noOfDim, noQueries);

                    Benchmark benchmark = new Benchmark(q, r, s);
                    benchmark.runBenchmark();
                }
            } finally {
                AsqldbConnection.close();
            }
        }

    }
}
