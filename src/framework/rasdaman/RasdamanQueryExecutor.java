package framework.rasdaman;

import data.DataGenerator;
import framework.QueryExecutor;
import framework.context.BenchmarkContext;
import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import util.DomainUtil;
import util.IO;
import util.Pair;
import util.ProcessExecutor;
import util.StopWatch;

/**
 * @author George Merticariu
 */
public class RasdamanQueryExecutor extends QueryExecutor<RasdamanSystem> {

    public RasdamanQueryExecutor(RasdamanSystem systemController, BenchmarkContext benchmarkContext) {
        super(systemController, benchmarkContext);
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        List<String> commandList = new ArrayList<>();
        commandList.add(systemController.getQueryCommand());
        commandList.add("-q");
        commandList.add(query);
        commandList.add("--user");
        commandList.add(systemController.getUser());
        commandList.add("--passwd");
        commandList.add(systemController.getPassword());
        if (systemController.getQueryCommand().contains("directql")) {
            commandList.add("-d");
            commandList.add(IO.concatPaths(systemController.getDataDir(), "RASBASE"));
        }
        Collections.addAll(commandList, args);

        StopWatch timer = new StopWatch();
        int status = ProcessExecutor.executeShellCommand(commandList.toArray(new String[]{}));
        long result = timer.getElapsedTime();

        if (status != 0) {
            if (!benchmarkContext.isCachingBenchmark()) {
                System.out.println("failed, restarting system...");
                systemController.restartSystem();
            } else {
                System.out.println("failed, retrying...");
            }
            timer.reset();
            status = ProcessExecutor.executeShellCommand(commandList.toArray(new String[]{}));
            result = timer.getElapsedTime();
            if (status != 0) {
                throw new Exception(String.format("Query execution failed with status %d", status));
            }
        }

        return result;
    }
    
}
