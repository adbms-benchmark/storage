package driver;

import framework.Benchmark;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.asqldb.AsqldbQueryExecutor;
import framework.asqldb.AsqldbQueryGenerator;
import framework.asqldb.AsqldbSystemController;
import framework.sciql.SciQLConnection;
import framework.sciql.SciQLQueryExecutor;
import framework.sciql.SciQLQueryGenerator;
import framework.sciql.SciQLSystemController;
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
        AsqldbSystemController asqldbSysController = new AsqldbSystemController("conf/system.properties");
        SciQLSystemController sciqlSysController = new SciQLSystemController("conf/system.properties");
        
        System.out.println("---------------------------------------------------");
        System.out.println("Benchmark configuration");
        System.out.println("ASQLDB " + asqldbContext);
        System.out.println("SciQL " + sciqlContext);
        System.out.println(benchContext);
        System.out.println(asqldbSysController);
        System.out.println(sciqlSysController);
        System.out.println("---------------------------------------------------");

        AsqldbConnection.open(asqldbContext.getUrl());
        SciQLConnection.open(sciqlContext);

        int noQueries = 3;

        try {
            for (int noOfDim = 1; noOfDim <= 3; ++noOfDim) {
                {
                    System.out.println("ASQLDB: " + noOfDim + "D");

                    AsqldbQueryExecutor queryExecutor = new AsqldbQueryExecutor(asqldbContext, asqldbSysController, benchContext, noOfDim);
                    AsqldbQueryGenerator queryGenerator = new AsqldbQueryGenerator(benchContext, noOfDim, noQueries);

                    Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, asqldbSysController);
                    benchmark.runBenchmark();
                }
                {
                    System.out.println("SciQL: " + noOfDim + "D");

                    SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlContext, sciqlSysController, benchContext, noOfDim);
                    SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext, noOfDim, noQueries);

                    Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, sciqlSysController);
                    benchmark.runBenchmark();
                }
            }
        } finally {
            SciQLConnection.close();
            AsqldbConnection.close();
        }

    }
}
