package benchmark.operations;

import benchmark.AdbmsSystem;
import benchmark.Driver;
import com.martiansoftware.jsap.*;
import util.DomainUtil;
import util.Pair;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.SimpleJSAP;


import java.io.IOException;

/**
 * Created by Danut Rusu on 23.03.17.
 */
public class OperationsBenchmarkDriver extends Driver {

    @Override
    protected SimpleJSAP getCmdLineConfig(Class c) throws JSAPException {
        SimpleJSAP jsap = new SimpleJSAP(
                getMainName(c),
                getDescription(),
                new Parameter[]{
                        new FlaggedOption("system", JSAP.STRING_PARSER, "rasdaman,scidb,sciql", JSAP.REQUIRED,
                                's', "systems", "Array DBMS to target in this run.").setList(true).setListSeparator(','),
                        new FlaggedOption("config", JSAP.STRING_PARSER, "conf/rasdaman.properties,conf/scidb.properties,conf/sciql.properties", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                                "system-configs", "System configuration (connection details, directories, etc).").setList(true).setListSeparator(','),
                        new FlaggedOption("dimension", JSAP.INTEGER_PARSER, "2", JSAP.REQUIRED,
                                'd', "dimensions", "Data dimensionality to be tested.").setList(true).setListSeparator(','),
                        new FlaggedOption("size", JSAP.STRING_PARSER, "1kB", JSAP.REQUIRED,
                                'b', "sizes", "Data sizes to be tested, as a number followed by B,kB,MB,GB,TB,PB,EB.").setList(true).setListSeparator(','),
                        new FlaggedOption("repeat", JSAP.INTEGER_PARSER, "3", JSAP.REQUIRED,
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
                        new FlaggedOption("datatype", JSAP.STRING_PARSER, "char", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                                "datatype", "Data type, set it with --datatype (char, float, double, long(rasdaman), int32(scidb), unsigned long(rasdaman), uint32(scidb)"),
                        new Switch("load", JSAP.NO_SHORTFLAG,
                                "load", "Load data."),
                        new Switch("drop", JSAP.NO_SHORTFLAG,
                                "drop", "Drop data."),
                        new Switch("generate", JSAP.NO_SHORTFLAG,
                                "generate", "Generate benchmark data."),
                        new Switch("nobenchmark", JSAP.NO_SHORTFLAG,
                                "disable-benchmark", "Do not run benchmark, just create or drop data."),
                        new Switch("norestart", JSAP.NO_SHORTFLAG,
                                "disable-restart", "Do not restart the benchmarked systems."),
                        new Switch("verbose",
                                'v', "verbose", "Print extra information.")
                }
        );
        return jsap;
    }

    @Override
    protected int runBenchmark(JSAPResult config) throws IOException {
        int exitCode = 0;

        double maxSelectSize = config.getDouble("max_select_size");
        String dataType = config.getString("datatype");
        Pair<Long, String> tileSize = DomainUtil.parseSize(config.getString("tilesize"));
        int queries = config.getInt("queries");
        int repeat = config.getInt("repeat");
        String datadir = config.getString("datadir");
        int timeout = config.getInt("timeout");

        OperationsBenchmarkContext benchmarkContext = new OperationsBenchmarkContext(maxSelectSize, tileSize.getFirst(), queries, repeat, datadir, timeout);
        benchmarkContext.setLoadData(config.getBoolean("load"));
        benchmarkContext.setDropData(config.getBoolean("drop"));
        benchmarkContext.setGenerateData(config.getBoolean("generate"));
        benchmarkContext.setDisableBenchmark(config.getBoolean("nobenchmark"));
        benchmarkContext.setDisableSystemRestart(config.getBoolean("norestart"));
        benchmarkContext.setDataType(dataType);

        String[] systems = config.getStringArray("system");
        String[] configs = config.getStringArray("config");
        if (systems.length != configs.length) {
            throw new IllegalArgumentException(systems.length + " systems specified, but " + configs.length + " system configuration files.");
        }

        int configInd = 0;

        String[] arraySizes = config.getStringArray("size");
        Pair<Long, String>[] sizes = DomainUtil.parseSizes(arraySizes);
        int[] dimensions = config.getIntArray("dimension");

        for (String system : systems) {
            String configFile = configs[configInd++];
            AdbmsSystem adbmsSystem = AdbmsSystem.getAdbmsSystem(system, configFile, benchmarkContext);

            int sizeInd = 0;
            for (Pair<Long, String> size : sizes) {
                String arraySizeShort = arraySizes[sizeInd++];
                benchmarkContext.setArraySize(size.getFirst());
                benchmarkContext.setArraySizeShort(arraySizeShort);

                for (int dimension : dimensions) {
                    benchmarkContext.setArrayDimensionality(dimension);
                    benchmarkContext.updateArrayName();
                    exitCode += runBenchmark(benchmarkContext, adbmsSystem);
                }
            }
        }
        return exitCode;
    }

    public static void main(String... args) throws Exception {
        OperationsBenchmarkDriver driver = new OperationsBenchmarkDriver();
        System.exit(driver.runMain(driver, args));
    }

    @Override
    protected String getDescription() {
        return "Benchmark operations behaviour in Array Databases. Currently supported systems: rasdaman, scidb";
    }
}
