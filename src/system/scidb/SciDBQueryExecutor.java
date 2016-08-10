package system.scidb;

import benchmark.QueryExecutor;
import benchmark.BenchmarkContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import util.ProcessExecutor;
import util.StopWatch;

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
        commandList.add(systemController.getQueryCommand());
        commandList.add("-q");
        commandList.add(query);
        commandList.add("-a");
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
