package driver;

import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.context.RasdamanContext;
import framework.context.SciDBContext;
import framework.rasdaman.RasdamanQueryExecutor;
import framework.rasdaman.RasdamanQueryGenerator;
import framework.rasdaman.RasdamanSystemController;
import framework.scidb.SciDBAQLQueryGenerator;
import framework.scidb.SciDBQueryExecutor;
import framework.scidb.SciDBSystemController;
import framework.sciql.SciQLQueryExecutor;
import framework.sciql.SciQLQueryGenerator;
import framework.sciql.SciQLSystemController;
import java.io.File;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author George Merticariu
 */
public class BenchmarkMain {

    private static void printUsage(String... args) {
        StringBuilder sb = new StringBuilder();
        String fileName = new File(BenchmarkMain.class.getProtectionDomain().getCodeSource().getLocation().getFile()).getName();
        sb.append("Usage:\n");
        sb.append(String.format("Benchmark rasdaman: java -jar %s %s\n", fileName, "rasdaman"));
        sb.append(String.format("Benchmark SciDB: java -jar %s %s\n", fileName, "SciDB"));
        sb.append(String.format("Benchmark SciQL: java -jar %s %s\n", fileName, "SciQL"));
        System.out.print(sb.toString());
    }

    public static void main(String... args) throws Exception {
        //TODO-GM: read contex configuration for each system from a config file
        //TODO-GM: retry-on-failure (restart system, run query)
        //TODO-GM: use context parameters


        if (args.length != 1) {
            printUsage(args);
            return;
        }

        switch (args[0]) {
            case "rasdaman": {

                RasdamanContext scidbContext = new RasdamanContext("conf/rasdaman.properties");
                BenchmarkContext benchContext = new BenchmarkContext("conf/benchmark.properties");
                int noQueries = 10;

                Map<Long, String> collectionSizes = new TreeMap<>();
                collectionSizes.put(1024l, "1Kb");
                collectionSizes.put(102400l, "100Kb");
                collectionSizes.put(1048576l, "1Mb");
                collectionSizes.put(10485760l, "10Mb");
                collectionSizes.put(104857600l, "100Mb");
                collectionSizes.put(1073741824l, "1Gb");
                collectionSizes.put(10737418240l, "10Gb");

                for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {

                    System.out.println("Rasdaman Benchmarking: " + noOfDim + "D");
                    {
                        for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                            String colName = String.format("colD%dS%s", noOfDim, longStringEntry.getValue());
                            long collectionSize = longStringEntry.getKey();
                            long maxSelectSize = (long) ((double) collectionSize / 10.0);

                            benchContext.setCollName1(colName);
                            benchContext.setCollSize(collectionSize);
                            benchContext.setMaxQuerySelectSize(maxSelectSize);

                            RasdamanSystemController s = new RasdamanSystemController(scidbContext.getStartCommand(), scidbContext.getStopCommand(), scidbContext.getRasdlBin());
                            RasdamanQueryExecutor r = new RasdamanQueryExecutor(scidbContext, s, benchContext, noOfDim);
                            RasdamanQueryGenerator q = new RasdamanQueryGenerator(benchContext, noOfDim, noQueries);

                            s.restartSystem();
                            r.createCollection();
//                            Benchmark benchmark = new Benchmark(q, r, s);
//                            benchmark.runBenchmark(collectionSize, maxSelectSize);

                        }
                    }
                }


                break;
            }
            case "SciDB": {

                SciDBContext scidbContext = new SciDBContext("conf/scidb.properties");
                BenchmarkContext benchContext = new BenchmarkContext("conf/benchmark.properties");
                int noQueries = 10;

                Map<Long, String> collectionSizes = new TreeMap<>();
                collectionSizes.put(1024l, "1Kb");
                collectionSizes.put(102400l, "100Kb");
                collectionSizes.put(1048576l, "1Mb");
                collectionSizes.put(10485760l, "10Mb");
                collectionSizes.put(104857600l, "100Mb");
                collectionSizes.put(1073741824l, "1Gb");
                collectionSizes.put(10737418240l, "10Gb");

                for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {

                    System.out.println("SciDB Benchmarking: " + noOfDim + "D");
                    {
                        for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                            String colName = String.format("colD%dS%s", noOfDim, longStringEntry.getValue());
                            long collectionSize = longStringEntry.getKey();
                            long maxSelectSize = (long) ((double) collectionSize / 10.0);

                            benchContext.setCollName1(colName);
                            benchContext.setCollSize(collectionSize);
                            benchContext.setMaxQuerySelectSize(maxSelectSize);

                            SciDBQueryExecutor r = new SciDBQueryExecutor(scidbContext, benchContext, noOfDim);
                            SciDBAQLQueryGenerator q = new SciDBAQLQueryGenerator(benchContext, noOfDim, noQueries);
                            SciDBSystemController s = new SciDBSystemController(scidbContext.getStartCommand(), scidbContext.getStopCommand());

                            s.restartSystem();
                            r.createCollection();
//                            Benchmark benchmark = new Benchmark(q, r, s);
//                            benchmark.runBenchmark(collectionSize, maxSelectSize);

                        }
                    }
                }

                break;
            }
            case "SciQL": {

                Map<Long, String> collectionSizes = new TreeMap<>();
//                collectionSizes.put(1024l, "1Kb");
//                collectionSizes.put(102400l, "100Kb");
//                collectionSizes.put(1048576l, "1Mb");
                collectionSizes.put(104857600l, "100Mb");
//                collectionSizes.put(1073741824l, "1Gb");
//                collectionSizes.put(10737418240l, "10Gb");

                ConnectionContext sciqlConnection = new ConnectionContext("conf/sciql.properties");
                BenchmarkContext benchContext = new BenchmarkContext("conf/benchmark.properties");
                int noQueries = 10;

                for (int noOfDim = 6; noOfDim <= 6; ++noOfDim) {
                    for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                        System.out.println("SciQLQueryExecutor: " + noOfDim + "D");
                        String colName = String.format("cold%ds%s", noOfDim, longStringEntry.getValue());
                        long collectionSize = longStringEntry.getKey();
                        long maxSelectSize = (long) ((double) collectionSize / 10.0);

                        benchContext.setCollName1(colName);
                        benchContext.setCollSize(collectionSize);
                        benchContext.setMaxQuerySelectSize(maxSelectSize);

                        SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext, noOfDim, noQueries);
                        SciQLSystemController systemController = new SciQLSystemController("conf/system.properties", sciqlConnection);
                        SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlConnection, systemController, benchContext, noOfDim);

                        systemController.restartSystem();
                        queryExecutor.createCollection();
//                        Benchmark benchmark = new Benchmark(queryGenerator, queryExecutor, systemController);
//                        benchmark.runBenchmark(benchContext.getCollSize(), benchContext.getMaxQuerySelectSize());
                    }
                }

                break;
            }
            default: {
                printUsage(args);
                return;
            }
        }
    }

}

