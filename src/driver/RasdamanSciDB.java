package driver;

import framework.Benchmark;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.context.SciDBContext;
import framework.scidb.SciDBQueryExecutor;
import framework.scidb.SciDBQueryGenerator;
import framework.scidb.SciDBSystemController;

/**
 * @author George Merticariu
 */
public class RasdamanSciDB {

    public static void main(String... args) throws Exception {
        //TODO-GM: read contex configuration for each system from a config file
        //TODO-GM: retry-on-failure (restart system, run query)
        //TODO-GM: use context parameters

        SciDBContext scidbContext = new SciDBContext("/conf/scidb.properties");

        ConnectionContext rasdamanContext = new ConnectionContext("rasadmin", "rasadmin", "", 35000, "RASBASE");
        BenchmarkContext benchContext = new BenchmarkContext("/conf/benchmark.properties");
        int noQueries = 10;


        //TODO-GM: create a list of config files representing each system to benchmark and read the list of configs
        //e.g. config/rasdaman1.conf, config/rasdaman2.conf, config/scidb.conf => do benchmarks on rasdama1 system, rasdaman2 system and scidb system

        for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {

//            System.out.println("RASDAMAN: " + noOfDim + "D");
//            {
//                RasdamanSystemController s = new RasdamanSystemController(new String[]{"/home/rasdaman/install/bin/start_rasdaman.sh"}, new String[]{"/home/rasdaman/install/bin/stop_rasdaman.sh"}, "/home/rasdaman/install/bin/rasdl");
//                RasdamanQueryExecutor r = new RasdamanQueryExecutor(rasdamanContext, s, benchContext, noOfDim);
//                RasdamanQueryGenerator q = new RasdamanQueryGenerator(benchContext, noOfDim, noQueries);
//
//                Benchmark benchmark = new Benchmark(q, r, s);
//                benchmark.runBenchmark();
//            }

            System.out.println("SciDBQueryExecutor: " + noOfDim + "D");
            {
                SciDBQueryExecutor r = new SciDBQueryExecutor(scidbContext, benchContext, noOfDim);
                SciDBQueryGenerator q = new SciDBQueryGenerator(benchContext, noOfDim, noQueries);
                SciDBSystemController s = new SciDBSystemController(scidbContext.getStartCommand(), scidbContext.getStopCommand());

                Benchmark benchmark = new Benchmark(q, r, s);
                benchmark.runBenchmark();
            }
        }

    }

}

