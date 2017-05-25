package system.scidb;

import benchmark.BenchmarkContext;
import benchmark.QueryExecutor;
import util.ProcessExecutor;
import util.StopWatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBQueryExecutor extends QueryExecutor<SciDBSystem> {
    
    public SciDBQueryExecutor(BenchmarkContext benchContext, SciDBSystem systemController) {
        super(systemController, benchContext);
        super.systemController = systemController;
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        List<String> commandList = new ArrayList<>();
//        query = "'" + query + "'";
        commandList.add(systemController.getQueryCommand());
        commandList.add("-q");
        commandList.add(query);
//        commandList.add("-a"); //only if AFL
        commandList.add("-n");
        commandList.add("-p");
        commandList.add(String.valueOf(systemController.getPort()));
        Collections.addAll(commandList, args);

        StopWatch timer = new StopWatch();
        int status = ProcessExecutor.executeShellCommand(commandList.toArray(new String[]{}));
        long result = timer.getElapsedTime();

        if (status != 0) {
            throw new Exception(String.format("Query execution failed with status %d", status));
        }

        return result;
    }
}
