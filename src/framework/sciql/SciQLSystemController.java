package framework.sciql;

import framework.SystemController;
import framework.context.ConnectionContext;
import java.io.IOException;
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

    public SciQLSystemController(String propertiesPath, ConnectionContext connContext) throws IOException {
        super(propertiesPath, connContext);
        this.systemName = "SciQL";
        this.sciqlHome = getValue(KEY_SCIQL_HOME);
        this.sciqlDbfarm = getValue(KEY_SCIQL_DBFARM);
        this.sciqlBinDir = IO.concatPaths(sciqlHome, "bin");
        this.sciqlMclientPath = IO.concatPaths(sciqlBinDir, "mclient");
        this.startSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "start", sciqlDbfarm};
        this.stopSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "stop", sciqlDbfarm};
    }

    @Override
    public void restartSystem() throws Exception {
        SciQLConnection.close();
//        if (executeShellCommand(stopSystemCommand) != 0) {
//            // ignore, it may be already stopped
//        }
//        Thread.sleep(8000);
//        if (executeShellCommand(startSystemCommand) != 0) {
//            throw new Exception("Failed to start the system.");
//        }
//        Thread.sleep(1000);
        SciQLConnection.open(connContext);
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
