package framework.scidb;

import framework.SystemController;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author George Merticariu
 */
public class SciDBSystemController extends SystemController {

    public SciDBSystemController(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath, "SciDB");
    }

    public SciDBSystemController(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand) throws IOException {
        super(propertiesPath, startSystemCommand, stopSystemCommand, "SciDB");
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
