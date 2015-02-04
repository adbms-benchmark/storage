package framework.sciql;

import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;

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

    public SciQLSystem(String propertiesPath) throws IOException {
        super(propertiesPath, "SciQL");
        this.sciqlBinDir = IO.concatPaths(installDir, "bin");
        this.sciqlMclientPath = IO.concatPaths(sciqlBinDir, "mclient");
        this.startCommand = new String[]{IO.concatPaths(sciqlBinDir, "monetdbd"), "start", dataDir};
        this.stopCommand = new String[]{IO.concatPaths(sciqlBinDir, "monetdbd"), "stop", dataDir};
        this.merovingianLockFile = IO.concatPaths(dataDir, ".merovingian_lock");
    }

    @Override
    public void restartSystem() throws Exception {
        stopSystem();
        startSystem();
    }

    private void stopSystem() throws Exception {
        log.debug("Stopping " + systemName);
        SciQLConnection.close();
        if (executeShellCommand(stopCommand) != 0) {
            // ignore, it may be already stopped
        }
        waitUntilLockRemoved();
    }

    private void startSystem() throws Exception {
        log.debug("Starting " + systemName);
        int retry = 0;
        while (executeShellCommand(startCommand) != 0) {
            if (retry++ == 5) {
                throw new Exception("Failed starting monetdb.");
            }
            executeShellCommand(stopCommand);
            waitUntilLockRemoved();
        }
        SciQLConnection.open(this);
    }

    private void waitUntilLockRemoved() throws Exception {
        File lock = new File(merovingianLockFile);
        while (lock.exists()) {
            Thread.sleep(500);
        }
        // just in case
        String pid = executeShellCommandOutput("pgrep", "mserver5");
        if (!"".equals(pid)) {
            executeShellCommand("pkill", "mserver5");
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
}
