package system.scidb;

import benchmark.AdbmsSystem;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.ProcessExecutor;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(SciDBSystem.class);

    private static final String SYSTEM_CONTROL_KEY = "bin.system";
    private static final String CLUSTER_NAME_KEY = "cluster.name";

    public SciDBSystem(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath, "SciDB");

        String clusterName = getValue(CLUSTER_NAME_KEY);
        String systemControl = getValue(SYSTEM_CONTROL_KEY);

        String binDir = IO.concatPaths(installDir, "bin");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, systemControl), startBin, clusterName};
        this.stopCommand = new String[]{IO.concatPaths(binDir, systemControl), stopBin, clusterName};
    }

    @Override
    public void restartSystem() throws Exception {
        log.debug("restarting " + systemName);
        if (ProcessExecutor.executeShellCommand(stopCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (ProcessExecutor.executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new SciDBAFLQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new SciDBQueryExecutor(benchmarkContext, this);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isCachingBenchmark()) {
            return new SciDBCachingBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isStorageBenchmark()) {
            return new SciDBStorageBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }

    @Override
    public void setSystemCacheSize(long bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
