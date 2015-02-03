package framework.sciql;

import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import org.asqldb.util.TimerUtil;
import util.IO;

/**
 *
 * @author George Merticariu
 */
public class SciQLSystem extends AdbmsSystem {

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
        TimerUtil.clearTimers();
        TimerUtil.startTimer("time");
        System.out.print("restarting monetdb... ");
        SciQLConnection.close();

        if (executeShellCommand(stopCommand) != 0) {
            // ignore, it may be already stopped
        }
        waitUntilLockRemoved();

        if (executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed starting monetdb.");
        }
        Thread.sleep(500);

        SciQLConnection.open(this);
        String res = TimerUtil.stopTimer("time");
        System.out.println("ok, " + res + ".");
    }

    private void waitUntilLockRemoved() throws Exception {
        File lock = new File(merovingianLockFile);
        while (lock.exists()) {
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
}
