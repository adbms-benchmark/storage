package framework.sciql;

import framework.SystemController;
import framework.context.ConnectionContext;
import java.io.File;
import java.io.IOException;
import org.asqldb.util.TimerUtil;
import util.IO;

/**
 *
 * @author George Merticariu
 */
public class SciQLSystemController extends SystemController {

    public static final String KEY_SCIQL_HOME = "sciql.home";
    public static final String KEY_SCIQL_DBFARM = "sciql.dbfarm";

    protected String sciqlHome;
    protected String sciqlDbfarm;
    protected String sciqlBinDir;
    protected String sciqlMclientPath;
    protected String merovingianLockFile;

    public SciQLSystemController(String propertiesPath, ConnectionContext connContext) throws IOException {
        super(propertiesPath, connContext);
        this.systemName = "SciQL";
        this.sciqlHome = getValue(KEY_SCIQL_HOME);
        this.sciqlDbfarm = getValue(KEY_SCIQL_DBFARM);
        this.sciqlBinDir = IO.concatPaths(sciqlHome, "bin");
        this.sciqlMclientPath = IO.concatPaths(sciqlBinDir, "mclient");
        this.startSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "start", sciqlDbfarm};
        this.stopSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "stop", sciqlDbfarm};
        this.merovingianLockFile = IO.concatPaths(sciqlDbfarm, ".merovingian_lock");
    }

    @Override
    public void restartSystem() throws Exception {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("time");
        System.out.print("restarting monetdb... ");
        SciQLConnection.close();

        if (executeShellCommand(stopSystemCommand) != 0) {
            // ignore, it may be already stopped
        }
        waitUntilLockRemoved();
        Thread.sleep(500);

        if (executeShellCommand(startSystemCommand) != 0) {
            executeShellCommand(stopSystemCommand);
            waitUntilLockRemoved();
            if (executeShellCommand(startSystemCommand) != 0) {
                throw new Exception("Failed starting monetdb.");
            }
        }
        Thread.sleep(500);

        SciQLConnection.open(connContext);
        String res = TimerUtil.stopTimer("time");
        System.out.println("ok, " + res + ".");
    }

    private void waitUntilLockRemoved() throws Exception {
        File lock = new File(merovingianLockFile);
        int count = 0;
        while (lock.exists()) {
            Thread.sleep(500);
            ++count;
            if (count > 60) {
                throw new Exception("Failed restarting monetdb after 30s.");
            }
        }
    }

    public String getSciqlHome() {
        return sciqlHome;
    }

    public String getSciqlDbfarm() {
        return sciqlDbfarm;
    }

    public String getMclientPath() {
        return sciqlMclientPath;
    }
}
