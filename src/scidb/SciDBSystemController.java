package scidb;

import framework.SystemController;

/**
 *
 * @author George Merticariu
 */
public class SciDBSystemController extends SystemController {

    public SciDBSystemController(String[] startSystemCommand, String[] stopSystemCommand) {
        super(startSystemCommand, stopSystemCommand, "SciDB");
    }

    @Override
    public void restartSystem() throws Exception {
        if (executeShellCommand(stopSystemCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (executeShellCommand(startSystemCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
    }
}
