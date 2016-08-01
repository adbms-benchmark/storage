package framework.rasdaman;

import framework.AdbmsSystem;
import framework.DataManager;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.Pair;
import util.ProcessExecutor;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class RasdamanSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(RasdamanSystem.class);

    public RasdamanSystem(String propertiesPath) throws IOException {
        super(propertiesPath, RASDAMAN_SYSTEM_NAME);
        String binDir = IO.concatPaths(installDir, "bin");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, startBin)};
        this.stopCommand = new String[]{IO.concatPaths(binDir, stopBin)};
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
    
    public int executeRasqlQuery(String query) {
        log.debug("executing rasql cmd: " + query);
        return ProcessExecutor.executeShellCommand(queryCommand,
                "-q", query,
                "--user", getUser(),
                "--passwd", getPassword());
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new RasdamanQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new RasdamanQueryExecutor(this, benchmarkContext);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isCachingBenchmark()) {
            return new RasdamanCachingBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isStorageBenchmark()) {
            return new RasdamanStorageBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }
}
