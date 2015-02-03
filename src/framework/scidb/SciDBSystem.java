package framework.scidb;

import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;

/**
 *
 * @author George Merticariu
 */
public class SciDBSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(SciDBSystem.class);

    private static final String SYSTEM_CONTROL_KEY = "bin.system";
    private static final String CLUSTER_NAME_KEY = "cluster.name";

    public SciDBSystem(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath, "SciDB");

        String clusterName = getValue(CLUSTER_NAME_KEY);
        String systemControl = getValue(SYSTEM_CONTROL_KEY);

        String binDir = IO.concatPaths(installDir, "bin");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, systemControl), startBin, clusterName};
        this.stopCommand = new String[]{IO.concatPaths(binDir, systemControl), stopBin, clusterName};
    }

    @Override
    public void restartSystem() throws Exception {
        log.debug("restarting " + systemName);
        if (executeShellCommand(stopCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new SciDBAFLQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new SciDBQueryExecutor(benchmarkContext, this);
    }
}
