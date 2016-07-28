package driver;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import framework.AdbmsSystem;
import framework.BenchmarkExecutor;
import framework.DataManager;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import util.DomainUtil;
import util.Pair;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class Driver {

    protected static Logger log;

    protected static SimpleJSAP getCmdLineConfig(Class c) throws JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(
                getMainName(c),
                "Benchmark storage management in Array Databases. Currently supported systems: rasdaman, SciDB, SciQL, ASQLDB.",
                new Parameter[]{
                    new FlaggedOption("system", JSAP.STRING_PARSER, "rasdaman,scidb,sciql", JSAP.REQUIRED,
                            's', "systems", "Array DBMS to target in this run.").setList(true).setListSeparator(','),
                    new FlaggedOption("type", JSAP.STRING_PARSER, BenchmarkContext.TYPE_STORAGE, JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "type", "Benchmark type (" + BenchmarkContext.TYPE_STORAGE + ", " + BenchmarkContext.TYPE_SQLMDA 
                                    + ", " + BenchmarkContext.TYPE_CACHING + ").").setList(true).setListSeparator(','),
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
                    new FlaggedOption("max_select_size", JSAP.DOUBLE_PARSER, "10", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
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

    protected static String getMainName(Class c) {
        return "run.sh";
    }

    protected static void setupLogger(boolean verbose, Class c) {
        if (verbose) {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
        } else {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
        }
        System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
        log = LoggerFactory.getLogger(c);
    }

    protected static int runBenchmark(JSAPResult config) throws IOException {
        int exitCode = 0;

        double maxSelectSize = config.getDouble("max_select_size");
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
        String[] arraySizes = config.getStringArray("size");
        Pair<Long, String>[] sizes = DomainUtil.parseSizes(arraySizes);
        int[] dimensions = config.getIntArray("dimension");
        String[] benchmarkTypes = config.getStringArray("type");

        for (String system : systems) {
            String configFile = configs[configInd++];
            AdbmsSystem adbmsSystem = AdbmsSystem.getAdbmsSystem(system, configFile);

            int sizeInd = 0;
            for (Pair<Long, String> size : sizes) {
                String arraySizeShort = arraySizes[sizeInd++];
                benchmarkContext.setArraySize(size.getFirst());
                benchmarkContext.setArraySizeShort(arraySizeShort);

                for (String type : benchmarkTypes) {
                    benchmarkContext.setBenchmarkType(type);
                    if (benchmarkContext.isSqlMdaBenchmark()) {
                        benchmarkContext.setArrayDimensionality(2);
                        exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                    } else if (benchmarkContext.isStorageBenchmark()) {
                        for (int dimension : dimensions) {
                            benchmarkContext.setArrayDimensionality(dimension);
                            benchmarkContext.updateArrayName();
                            exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                        }
                    } else if (benchmarkContext.isCachingBenchmark()) {
                        exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                    }
                }
            }
        }

        return exitCode;
    }

    protected static int runBenchmark(BenchmarkContext benchmarkContext, AdbmsSystem systemController) throws IOException {
        int exitCode = 0;
        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchmarkContext);
        QueryExecutor queryExecutor = systemController.getQueryExecutor(benchmarkContext);
        DataManager dataManager = systemController.getDataManager(benchmarkContext, queryExecutor);
        BenchmarkExecutor benchmark = new BenchmarkExecutor(benchmarkContext, queryGenerator, queryExecutor, dataManager, systemController);
        try {
            benchmark.runBenchmark();
        } catch (Exception ex) {
            log.error("Failed executing benchmark.", ex);
            exitCode = 1;
        }
        return exitCode;
    }

    public static void main(String... args) throws Exception {
        SimpleJSAP jsap = getCmdLineConfig(Driver.class);
        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }
        setupLogger(config.getBoolean("verbose"), Driver.class);

        System.exit(runBenchmark(config));
    }

}
