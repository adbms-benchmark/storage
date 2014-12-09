package framework.sciql;

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

    public SciQLSystemController(String propertiesPath) throws IOException {
        super(propertiesPath);
        this.sciqlHome = getValue(KEY_SCIQL_HOME);
        this.sciqlDbfarm = getValue(KEY_SCIQL_DBFARM);
        String sciqlBinDir = IO.concatPaths(sciqlHome, "bin");
        this.startSystemCommand = new String[]{sciqlBinDir + "/monetdbd start " + sciqlDbfarm};
        this.stopSystemCommand = new String[]{sciqlBinDir + "/monetdbd stop " + sciqlDbfarm};
    }

    @Override
    public void restartSystem() throws Exception {
//        if (executeShellCommand(stopSystemCommand) != 0) {
//            throw new Exception("Failed to stop the system.");
//        }
//
//        if (executeShellCommand(startSystemCommand) != 0) {
//            throw new Exception("Failed to start the system.");
//        }
    }
}
