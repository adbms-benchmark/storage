package framework.sciql;

import framework.context.ConnectionContext;
import framework.SystemController;
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

    public SciQLSystemController(String propertiesPath, ConnectionContext connContext) throws IOException {
        super(propertiesPath, connContext);
        this.systemName = "SciQL";
        this.sciqlHome = getValue(KEY_SCIQL_HOME);
        this.sciqlDbfarm = getValue(KEY_SCIQL_DBFARM);
        String sciqlBinDir = IO.concatPaths(sciqlHome, "bin");
        this.startSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "start", sciqlDbfarm};
        this.stopSystemCommand = new String[]{sciqlBinDir + "/monetdbd", "stop", sciqlDbfarm};
    }

    @Override
    public void restartSystem() throws Exception {
        SciQLConnection.close();
        if (executeShellCommand(stopSystemCommand) != 0) {
            // ignore, it may be already stopped
        }
        Thread.sleep(8000);
        if (executeShellCommand(startSystemCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
        Thread.sleep(1000);
        SciQLConnection.open(connContext);
    }
}
