import framework.Benchmark;
import framework.Configuration;
import framework.Context;
import rasdaman.RasdamanQueryExecutor;
import rasdaman.RasdamanQueryGenerator;
import rasdaman.RasdamanSystemController;
import scidb.SciDBQueryExecutor;
import scidb.SciDBQueryGenerator;
import scidb.SciDBSystemController;

/**
 *
 * @author George Merticariu
 */
public class MainApp {

    public static void main(String... args) throws Exception {

        //TODO-GM: read contex configuration for each system from a config file
        //TODO-GM: retry-on-failure (restart system, run query)
        //TODO-GM: use context parameters

        Context rasdamanContext = new Context("rasadmin", "rasadmin", "", 35000, "RASBASE");
        Context scidibContext = new Context("", "", "", 35000, "");
        int noQueries = 10;


        //TODO-GM: create a list of config files representing each system to benchmark and read the list of configs
        //e.g. config/rasdaman1.conf, config/rasdaman2.conf, config/scidb.conf => do benchmarks on rasdama1 system, rasdaman2 system and scidb system

        for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {

            System.out.println("RASDAMAN: " + noOfDim + "D");
            {
                RasdamanSystemController s = new RasdamanSystemController(new String[]{"/home/rasdaman/install/bin/start_rasdaman.sh"}, new String[]{"/home/rasdaman/install/bin/stop_rasdaman.sh"}, "/home/rasdaman/install/bin/rasdl");
                RasdamanQueryExecutor r = new RasdamanQueryExecutor(rasdamanContext, s, noOfDim);
                RasdamanQueryGenerator q = new RasdamanQueryGenerator(Configuration.COLLECTION_SIZE, noOfDim, Configuration.MAX_SELECT_SIZE, noQueries);

                Benchmark benchmark = new Benchmark(q, r, s);
                benchmark.runBenchmark();
            }

            System.out.println("SCIDB: " + noOfDim + "D");
            {
                SciDBQueryExecutor r = new SciDBQueryExecutor(scidibContext, noOfDim);
                SciDBQueryGenerator q = new SciDBQueryGenerator(Configuration.COLLECTION_SIZE, noOfDim, Configuration.MAX_SELECT_SIZE, noQueries);
                SciDBSystemController s = new SciDBSystemController(new String[]{"/opt/scidb/14.8/bin/scidb.py", "stopall", "cluster"}, new String[]{"/opt/scidb/14.8/bin/scidb.py", "startall", "cluster"});

                Benchmark benchmark = new Benchmark(q, r, s);
                benchmark.runBenchmark();
            }
        }

    }
}

