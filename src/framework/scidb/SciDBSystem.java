package framework.scidb;

import framework.QueryGenerator;
import framework.AdbmsSystem;
import framework.context.BenchmarkContext;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author George Merticariu
 */
public class SciDBSystem extends AdbmsSystem {

    public SciDBSystem(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath, "SciDB");
    }

    public SciDBSystem(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand) throws IOException {
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

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new SciDBAFLQueryGenerator(benchmarkContext);
    }
}
