package driver;

import framework.Benchmark;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.context.RasdamanContext;
import framework.context.SciDBContext;
import framework.rasdaman.RasdamanQueryExecutor;
import framework.rasdaman.RasdamanQueryGenerator;
import framework.rasdaman.RasdamanSystemController;
import framework.scidb.SciDBQueryExecutor;
import framework.scidb.SciDBQueryGenerator;
import framework.scidb.SciDBSystemController;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author George Merticariu
 */
public class RasdamanSciDB {

    public static void main(String... args) throws Exception {
        //TODO-GM: read contex configuration for each system from a config file
        //TODO-GM: retry-on-failure (restart system, run query)
        //TODO-GM: use context parameters

        SciDBContext scidbContext = new SciDBContext("/conf/scidb.properties");

        RasdamanContext rasdamanContext = new RasdamanContext("/conf/rasdaman.properties");
        BenchmarkContext benchContext = new BenchmarkContext("/conf/benchmark.properties");
        int noQueries = 10;


        //TODO-GM: create a list of config files representing each system to benchmark and read the list of configs
        //e.g. config/rasdaman1.conf, config/rasdaman2.conf, config/scidb.conf => do benchmarks on rasdama1 system, rasdaman2 system and scidb system


        Map<Long, String> collectionSizes = new TreeMap<>();
        collectionSizes.put(1024l, "1Kb");
        collectionSizes.put(102400l, "100Kb");
        collectionSizes.put(1048576l, "1Mb");
        collectionSizes.put(104857600l, "100Mb");
        collectionSizes.put(1073741824l, "1Gb");
//        a.put(10737418240l, "10Gb");

        for (int noOfDim = 4; noOfDim <= 6; ++noOfDim) {

            System.out.println("Benchmarking: " + noOfDim + "D");
            {
                for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                    String colName = String.format("colD%dS%s", noOfDim, longStringEntry.getValue());
                    long collectionSize = longStringEntry.getKey();
                    long maxSelectSize = (long) ((double) collectionSize / 10.0);

                    if (collectionSize < 104857600l && noOfDim == 4){
                        continue;
                    }

                    benchContext.setCollName1(colName);
                    benchContext.setCollSize(collectionSize);
                    benchContext.setMaxQuerySelectSize(maxSelectSize);

                    SciDBQueryExecutor r = new SciDBQueryExecutor(scidbContext, benchContext, noOfDim);
                    SciDBQueryGenerator q = new SciDBQueryGenerator(benchContext, noOfDim, noQueries);
                    SciDBSystemController s = new SciDBSystemController(scidbContext.getStartCommand(), scidbContext.getStopCommand());

                    Benchmark benchmark = new Benchmark(q, r, s);
                    benchmark.runBenchmark(noOfDim, collectionSize, maxSelectSize);

//                    RasdamanSystemController rsc = new RasdamanSystemController(rasdamanContext.getStartCommand(), rasdamanContext.getStopCommand(), rasdamanContext.getRasdlBin());
//                    RasdamanQueryExecutor rqe = new RasdamanQueryExecutor(rasdamanContext, rsc, benchContext, noOfDim);
//                    RasdamanQueryGenerator rqg = new RasdamanQueryGenerator(benchContext, noOfDim, noQueries);
//
//                    Benchmark benchmark = new Benchmark(rqg,rqe,rsc);
//                    benchmark.runBenchmark(noOfDim, collectionSize, maxSelectSize);
                }
            }
        }

    }

}

