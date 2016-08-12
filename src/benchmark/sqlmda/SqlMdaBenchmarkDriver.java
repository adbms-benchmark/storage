package benchmark.sqlmda;

import benchmark.storage.StorageBenchmarkDriver;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import benchmark.AdbmsSystem;
import benchmark.BenchmarkContext;
import java.io.IOException;
import util.DomainUtil;
import util.Pair;

/**
 * Entry point for the caching benchmark.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SqlMdaBenchmarkDriver extends StorageBenchmarkDriver {

    @Override
    protected SimpleJSAP getCmdLineConfig(Class c) throws JSAPException {

        SimpleJSAP jsap = new SimpleJSAP(
                getMainName(c),
                getDescription(),
                new Parameter[]{
                    new FlaggedOption("system", JSAP.STRING_PARSER, "sciql,asqldb", JSAP.REQUIRED,
                            's', "systems", "Array DBMS to target in this run.").setList(true).setListSeparator(','),
                    new FlaggedOption("config", JSAP.STRING_PARSER, "conf/rasdaman.properties,conf/scidb.properties,conf/sciql.properties", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "system-configs", "System configuration (connection details, directories, etc).").setList(true).setListSeparator(','),
                    new FlaggedOption("size", JSAP.STRING_PARSER, "1kB", JSAP.REQUIRED,
                            'b', "sizes", "Data sizes to be tested, as a number followed by B,kB,MB,GB,TB,PB,EB.").setList(true).setListSeparator(','),
                    new FlaggedOption("repeat", JSAP.INTEGER_PARSER, "5", JSAP.REQUIRED,
                            'r', "repeat", "Times to repeat each test query."),
                    new FlaggedOption("timeout", JSAP.INTEGER_PARSER, "-1", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "timout", "Query timeout in seconds; -1 means no query timeout."),
                    new FlaggedOption("datadir", JSAP.STRING_PARSER, "/tmp", JSAP.REQUIRED, JSAP.NO_SHORTFLAG,
                            "datadir", "Data directory, for temporary and permanent data used in ingestion."),
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

        int repeat = config.getInt("repeat");
        String datadir = config.getString("datadir");
        int timeout = config.getInt("timeout");

        SqlMdaBenchmarkContext benchmarkContext = new SqlMdaBenchmarkContext(repeat, datadir, timeout);
        benchmarkContext.setLoadData(config.getBoolean("load"));
        benchmarkContext.setDropData(config.getBoolean("drop"));
        benchmarkContext.setGenerateData(config.getBoolean("generate"));
        benchmarkContext.setDisableBenchmark(config.getBoolean("nobenchmark"));
        benchmarkContext.setDisableSystemRestart(config.getBoolean("norestart"));

        String[] systems = config.getStringArray("system");
        String[] configs = config.getStringArray("config");
        if (systems.length != configs.length) {
            throw new IllegalArgumentException(systems.length + " systems specified, but " + configs.length + " system configuration files.");
        }
        int configInd = 0;
        String[] arraySizes = config.getStringArray("size");
        Pair<Long, String>[] sizes = DomainUtil.parseSizes(arraySizes);

        for (String system : systems) {
            String configFile = configs[configInd++];
            AdbmsSystem adbmsSystem = AdbmsSystem.getAdbmsSystem(system, configFile, benchmarkContext);

            int sizeInd = 0;
            for (Pair<Long, String> size : sizes) {
                String arraySizeShort = arraySizes[sizeInd++];
                benchmarkContext.setArraySize(size.getFirst());
                benchmarkContext.setArraySizeShort(arraySizeShort);
                exitCode += runBenchmark(benchmarkContext, adbmsSystem);
            }
        }

        return exitCode;
    }

    @Override
    protected String getDescription() {
        return "Benchmark SQL management in Array Databases. Currently supported systems: SciQL, ASQLDB.";
    }
    
    
    public static void main(String... args) throws Exception {
        SqlMdaBenchmarkDriver driver = new SqlMdaBenchmarkDriver();
        System.exit(driver.runMain(driver, args));
    }
}
