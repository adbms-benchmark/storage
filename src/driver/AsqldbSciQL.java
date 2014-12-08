package driver;

import framework.Benchmark;
import framework.Configuration;
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

        ConnectionContext asqldbContext = new ConnectionContext("SA", "", "jdbc:hsqldb:file:/var/hsqldb/benchmark;shutdown=true", -1, "benchmark");
        ConnectionContext sciqlContext = new ConnectionContext("", "", "", 35000, "");
        int noQueries = 10;

        try {
            for (int noOfDim = 1; noOfDim <= 3; ++noOfDim) {

                System.out.println("ASQLDB: " + noOfDim + "D");
                {
                    AsqldbSystemController s = new AsqldbSystemController();
                    AsqldbQueryExecutor r = new AsqldbQueryExecutor(asqldbContext, s, noOfDim);
                    AsqldbQueryGenerator q = new AsqldbQueryGenerator(Configuration.COLLECTION_SIZE, noOfDim, Configuration.MAX_SELECT_SIZE, noQueries);

                    Benchmark benchmark = new Benchmark(q, r, s);
                    benchmark.runBenchmark();
                }
            }
        } finally {
            AsqldbConnection.close();
        }

    }
}

