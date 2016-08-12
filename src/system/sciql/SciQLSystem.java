package system.sciql;

import benchmark.AdbmsSystem;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.ProcessExecutor;

/**
 * SciQL system manager.
 *
 * @author Dimitar Misev
 */
public class SciQLSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(SciQLSystem.class);

    protected String sciqlBinDir;
    protected String sciqlMclientPath;
    protected String merovingianLockFile;

    public SciQLSystem(String propertiesPath, BenchmarkContext benchmarkContext) throws IOException {
        super(propertiesPath, "SciQL", benchmarkContext);
        this.sciqlBinDir = IO.concatPaths(installDir, "bin");
        this.sciqlMclientPath = IO.concatPaths(sciqlBinDir, "mclient");
        this.startCommand = new String[]{IO.concatPaths(sciqlBinDir, "monetdbd"), "start", dataDir};
        this.stopCommand = new String[]{IO.concatPaths(sciqlBinDir, "monetdbd"), "stop", dataDir};
        this.merovingianLockFile = IO.concatPaths(dataDir, ".merovingian_lock");
    }

    @Override
    public void restartSystem() throws Exception {
        if (!benchmarkContext.isDisableSystemRestart()) {
            stopSystem();
            startSystem();
        }
    }
    
    private void stopSystem() throws Exception {
        log.debug("Stopping " + systemName);
        SciQLConnection.close();
        if (ProcessExecutor.executeShellCommand(stopCommand) != 0) {
            // ignore, it may be already stopped
        }
        waitUntilLockRemoved();
    }

    private void startSystem() throws Exception {
        log.debug("Starting " + systemName);
        while (ProcessExecutor.executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed starting monetdb.");
        }
        SciQLConnection.open(this);
        log.debug(" -> SciQL started.");
    }

    private void waitUntilLockRemoved() throws Exception {
        File lock = new File(merovingianLockFile);
        while (lock.exists() || !"".equals(ProcessExecutor.executeShellCommandOutput(true, "pgrep", "mserver5"))) {
            log.debug("lock exists " + lock.exists() + ", sleeping 500ms: " + lock.getAbsolutePath());
            Thread.sleep(500);
        }
    }

    public String getMclientPath() {
        return sciqlMclientPath;
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new SciQLQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new SciQLQueryExecutor(benchmarkContext, this);
    }


    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isSqlMdaBenchmark()) {
            return new SciQLSqlMdaBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }

    @Override
    public void setSystemCacheSize(long bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
