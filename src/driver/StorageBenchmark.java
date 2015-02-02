package driver;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import framework.Benchmark;
import framework.SystemController;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.context.RasdamanContext;
import framework.context.SciDBContext;
import framework.rasdaman.RasdamanQueryExecutor;
import framework.rasdaman.RasdamanQueryGenerator;
import framework.rasdaman.RasdamanSystemController;
import framework.scidb.SciDBAFLQueryGenerator;
import framework.scidb.SciDBQueryExecutor;
import framework.scidb.SciDBSystemController;
import framework.sciql.SciQLQueryExecutor;
import framework.sciql.SciQLQueryGenerator;
import framework.sciql.SciQLSystemController;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import util.DomainUtil;
import util.Pair;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class StorageBenchmark {

    private static Logger logger;

    public static void main(String... args) throws Exception {

        SimpleJSAP jsap = getCmdLineConfig();
        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }
        setupLogger(config.getBoolean("verbose"));

        System.exit(runBenchmark(config));

        BenchmarkContext benchContext = new BenchmarkContext(10, 4000000, 6, 3, "/tmp", -1);
        int noQueries = 6;

        switch (args[0]) {
            case "rasdaman": {

                RasdamanContext rasdamanContext = new RasdamanContext("conf/rasdaman.properties");

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

                            benchContext.setArrayName(colName);
                            benchContext.setArraySize(collectionSize);

                            RasdamanSystemController s = new RasdamanSystemController("conf/rasdaman.properties");
                            RasdamanQueryExecutor r = new RasdamanQueryExecutor(rasdamanContext, s, benchContext, noOfDim);
                            RasdamanQueryGenerator q = new RasdamanQueryGenerator(benchContext, noOfDim, noQueries);

                            Benchmark benchmark = new Benchmark(benchContext, q, r, s);
                            benchmark.runBenchmark(collectionSize, maxSelectSize);
                        }
                    }
                }


                break;
            }
            case "SciDB": {

                SciDBContext scidbContext = new SciDBContext("conf/scidb.properties");

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

                            benchContext.setArrayName(colName);
                            benchContext.setArraySize(collectionSize);

                            SciDBQueryExecutor r = new SciDBQueryExecutor(scidbContext, benchContext, noOfDim);
                            SciDBAFLQueryGenerator q = new SciDBAFLQueryGenerator(benchContext, noOfDim, noQueries);
                            SciDBSystemController s = new SciDBSystemController("conf/scidb.properties");

                            Benchmark benchmark = new Benchmark(benchContext, q, r, s);
                            benchmark.runBenchmark(collectionSize, maxSelectSize);

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
//                collectionSizes.put(104857600l, "100Mb");
                collectionSizes.put(1073741824l, "1Gb");
//                collectionSizes.put(10737418240l, "10Gb");

                ConnectionContext sciqlConnection = new ConnectionContext("conf/sciql.properties");

                for (int noOfDim = 1; noOfDim <= 6; ++noOfDim) {
                    for (Map.Entry<Long, String> longStringEntry : collectionSizes.entrySet()) {
                        System.out.println("SciQLQueryExecutor: " + noOfDim + "D");
                        String colName = String.format("cold%ds%s", noOfDim, longStringEntry.getValue());
                        long collectionSize = longStringEntry.getKey();
                        long maxSelectSize = (long) ((double) collectionSize / 10.0);

                        benchContext.setArrayName(colName);
                        benchContext.setArraySize(collectionSize);

                        SciQLQueryGenerator queryGenerator = new SciQLQueryGenerator(benchContext, noOfDim, noQueries);
                        SciQLSystemController systemController = new SciQLSystemController("conf/sciql.properties");
                        SciQLQueryExecutor queryExecutor = new SciQLQueryExecutor(sciqlConnection, systemController, benchContext, noOfDim);

                        Benchmark benchmark = new Benchmark(benchContext, queryGenerator, queryExecutor, systemController);
                        benchmark.runBenchmark(benchContext.getArraySize(), benchContext.getMaxSelectSize());
                    }
                }

                break;
            }
            default: {
                return;
            }
        }
    }

    private static SimpleJSAP getCmdLineConfig() throws JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(
                getMainName(),
                "Benchmark storage management in Array Databases. Currently supported systems: rasdaman, SciDB, SciQL.",
                new Parameter[]{
                    new FlaggedOption("system", JSAP.STRING_PARSER, "rasdaman,scidb,sciql", JSAP.REQUIRED,
                            's', "systems", "Array DBMS to target in this run.").setList(true).setListSeparator(','),
                    new FlaggedOption("config", JSAP.STRING_PARSER, "conf/rasdaman.properties,conf/scidb.properties,conf/sciql.properties", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "system-configs", "System configuration (connection details, directories, etc).").setList(true).setListSeparator(','),
                    new FlaggedOption("dimension", JSAP.INTEGER_PARSER, "1,2,3,4,5,6", JSAP.REQUIRED,
                            'd', "dimensions", "Data dimensionality to be tested.").setList(true).setListSeparator(','),
                    new FlaggedOption("size", JSAP.STRING_PARSER, "1kB,100kB,1MB,100MB,1GB", JSAP.REQUIRED,
                            'b', "sizes", "Data sizes to be tested, as a number followed by B,kB,MB,GB,TB,PB,EB.").setList(true).setListSeparator(','),
                    new FlaggedOption("repeat", JSAP.INTEGER_PARSER, "5", JSAP.REQUIRED,
                            'r', "repeat", "Times to repeat each test query."),
                    new FlaggedOption("queries", JSAP.INTEGER_PARSER, "6", JSAP.REQUIRED,
                            'q', "queries", "Number of queries per query category."),
                    new FlaggedOption("max_select_size", JSAP.INTEGER_PARSER, "10", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "max-select-size", "Maximum select size, as percentage of the array size."),
                    new FlaggedOption("timeout", JSAP.INTEGER_PARSER, "-1", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "timout", "Query timeout in seconds; -1 means no query timeout."),
                    new FlaggedOption("tilesize", JSAP.STRING_PARSER, "4MB", JSAP.REQUIRED,
                            't', "tile-size", "Tile size, same format as for the --sizes option."),
                    new FlaggedOption("datadir", JSAP.STRING_PARSER, "/tmp", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "datadir", "Data directory, for temporary and permanent data used in ingestion."),
                    new Switch("create",
                            'c', "create", "Create data."),
                    new Switch("drop", JSAP.NO_SHORTFLAG,
                            "drop", "Drop data."),
                    new Switch("nobenchmark", JSAP.NO_SHORTFLAG,
                            "disable-benchmark", "Do not run benchmark, just create or drop data."),
                    new Switch("verbose",
                            'v', "verbose", "Print extra information.")
                }
        );
        return jsap;
    }

    private static String getMainName() {
        return new File(StorageBenchmark.class.getProtectionDomain().getCodeSource().getLocation().getFile()).getName();
    }

    private static void setupLogger(boolean verbose) {
        if (verbose) {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
        } else {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
        }
        logger = LoggerFactory.getLogger(StorageBenchmark.class);
    }

    private static int runBenchmark(JSAPResult config) throws IOException {
        int exitCode = 0;

        int maxSelectSize = config.getInt("max_select_size");
        Pair<Long, String> tileSize = DomainUtil.parseSize(config.getString("tilesize"));
        int queries = config.getInt("queries");
        int repeat = config.getInt("repeat");
        String datadir = config.getString("datadir");
        int timeout = config.getInt("timeout");
        
        BenchmarkContext benchmarkContext = new BenchmarkContext(maxSelectSize, tileSize.getFirst(), queries, repeat, datadir, timeout);
        benchmarkContext.setCreateData(config.getBoolean("create"));
        benchmarkContext.setDropData(config.getBoolean("drop"));
        benchmarkContext.setDisableBenchmark(config.getBoolean("nobenchmark"));

        String[] systems = config.getStringArray("system");
        String[] configs = config.getStringArray("config");
        if (systems.length != configs.length) {
            throw new IllegalArgumentException(systems.length + " systems specified, but " + configs.length + " system configuration files.");
        }
        int configInd = 0;
        Pair<Long, String>[] sizes = DomainUtil.parseSizes(config.getStringArray("size"));
        int[] dimensions = config.getIntArray("dimension");
        
        for (String system : systems) {
            SystemController systemController = SystemController.getSystemController(system, configs[configInd++]);
            for (Pair<Long, String> size : sizes) {
                for (int dimension : dimensions) {
                    
                }
            }
        }

        return exitCode;
    }

}
