package driver;

import framework.Benchmark;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.sciql.SciQLQueryExecutor;
import framework.sciql.SciQLQueryGenerator;
import framework.sciql.SciQLSystemController;

/**
 * @author George Merticariu
 */
public class SciQL {

    public static void main(String... args) throws Exception {
        //TODO-GM: read contex configuration for each system from a config file
        //TODO-GM: retry-on-failure (restart system, run query)
        //TODO-GM: use context parameters

        ConnectionContext sciqlConnection = new ConnectionContext("/conf/sciql.properties");
        BenchmarkContext benchContext = new BenchmarkContext("/conf/benchmark.properties");
        int noQueries = 10;

        for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {

            System.out.println("SciQLQueryExecutor: " + noOfDim + "D");
            {
                SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext, noOfDim, noQueries);
                SciQLSystemController systemController = new SciQLSystemController("/conf/system.properties", sciqlConnection);
                SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlConnection, systemController, benchContext, noOfDim);

                Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, systemController);
                benchmark.runBenchmark(noOfDim, benchContext.getCollSize(), benchContext.getMaxQuerySelectSize());
            }
        }

    }

}

