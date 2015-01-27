package driver;

import framework.Benchmark;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.sciql.SciQLQueryExecutor;
import framework.sciql.SciQLQueryGenerator;
import framework.sciql.SciQLSystemController;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author George Merticariu
 */
public class SciQL {

    public static void main(String... args) throws Exception {
        Map<Long, String> collectionSizes = new TreeMap<>();
        collectionSizes.put(1024l, "1Kb");
        collectionSizes.put(102400l, "100Kb");
        collectionSizes.put(1048576l, "1Mb");
        collectionSizes.put(104857600l, "100Mb");
        collectionSizes.put(1073741824l, "1Gb");
        collectionSizes.put(10737418240l, "10Gb");

        ConnectionContext sciqlConnection = new ConnectionContext("/conf/sciql.properties");
        BenchmarkContext benchContext = new BenchmarkContext("/conf/benchmark.properties");
        int noQueries = 10; 

        for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {
            for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                System.out.println("SciQLQueryExecutor: " + noOfDim + "D");
                String colName = String.format("colD%dS%s", noOfDim, longStringEntry.getValue());
                long collectionSize = longStringEntry.getKey();
                long maxSelectSize = (long) ((double) collectionSize / 10.0);

                benchContext.setCollName1(colName);
                benchContext.setCollSize(collectionSize);
                benchContext.setMaxQuerySelectSize(maxSelectSize);

                SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext, noOfDim, noQueries);
                SciQLSystemController systemController = new SciQLSystemController("/conf/system.properties", sciqlConnection);
                SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlConnection, systemController, benchContext, noOfDim);

                Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, systemController);
                benchmark.runBenchmark(noOfDim, benchContext.getCollSize(), benchContext.getMaxQuerySelectSize());
            }
        }

    }

}
