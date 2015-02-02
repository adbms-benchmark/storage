package driver;

import framework.Benchmark;
import framework.asqldb.AsqldbQueryExecutor;
import framework.asqldb.AsqldbQueryGenerator;
import framework.asqldb.AsqldbSystemController;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
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
        BenchmarkContext benchContext = new BenchmarkContext(10, 4000000, 6, 3, "/tmp", -1);
        AsqldbSystemController asqldbSysController = new AsqldbSystemController("conf/system.properties");
        SciQLSystemController sciqlSysController = new SciQLSystemController("conf/sciql.properties");

        System.out.println("---------------------------------------------------");
        System.out.println("Benchmark configuration");
        System.out.println("");
        System.out.println("ASQLDB " + asqldbContext);
        System.out.println("SciQL " + sciqlContext);
        System.out.println("");
        System.out.println(benchContext);
        System.out.println("");
        System.out.println(asqldbSysController);
        System.out.println(sciqlSysController);
        System.out.println("---------------------------------------------------");

        AsqldbConnection.open(asqldbContext.getUrl());
        SciQLConnection.open(sciqlContext);

        int noQueries = 1;
        int noOfDim = 2;

        try {
            {
                System.out.println("ASQLDB");

                AsqldbQueryExecutor queryExecutor = new AsqldbQueryExecutor(asqldbContext, asqldbSysController, benchContext, noOfDim);
                AsqldbQueryGenerator queryGenerator = new AsqldbQueryGenerator(benchContext);

                Benchmark benchmark = new Benchmark(benchContext, queryGenerator, queryExecutor, asqldbSysController);
//                benchmark.runBenchmark();
            }
            {
                System.out.println("SciQL");

                SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlContext, sciqlSysController, benchContext, noOfDim);
                SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext);

                Benchmark benchmark = new Benchmark(benchContext, queryGenerator, queryExecutor, sciqlSysController);
//                benchmark.runBenchmark();
            }
        } finally {
            SciQLConnection.close();
            AsqldbConnection.close();
        }

    }
}
