package framework.scidb;

import data.DataGenerator;
import framework.AdbmsSystem;
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

/**
 * @author George Merticariu
 */
public class SciDBQueryExecutor extends QueryExecutor<SciDBSystem> {
    
    private SciDBSystem systemController;

    public SciDBQueryExecutor(BenchmarkContext benchContext, SciDBSystem system) {
        super(system, benchContext);
        this.systemController = system;
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        List<String> commandList = new ArrayList<>();
        commandList.add(context.getQueryCommand());
        commandList.add("-q");
        commandList.add(query);
        commandList.add("-p");
        commandList.add(String.valueOf(context.getPort()));
        Collections.addAll(commandList, args);

        long startTime = System.currentTimeMillis();
        int status = AdbmsSystem.executeShellCommand(commandList.toArray(new String[]{}));
        long result = System.currentTimeMillis() - startTime;

        if (status != 0) {
            throw new Exception(String.format("Query execution failed with status %d", status));
        }

        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getArraySize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        long chunkUpperBound = DomainUtil.getDimensionUpperBound(noOfDimensions, benchContext.getCollTileSize());
        long chunkSize = chunkUpperBound + 1l;
        dataGenerator = new DataGenerator(fileSize, benchContext.getDataDir());
        String filePath = dataGenerator.getFilePath();

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(benchContext.getArrayName());
        createArrayQuery.append(" <");
        createArrayQuery.append(benchContext.getArrayName());
        createArrayQuery.append(":char>");
        createArrayQuery.append('[');

        boolean isFirst = true;
        for (int i = 0; i < domainBoundaries.size(); i++) {
            if (!isFirst) {
                createArrayQuery.append(",");
            }
            isFirst = false;
            createArrayQuery.append("axis");
            createArrayQuery.append(i);
            createArrayQuery.append("=");
            Pair<Long, Long> axisDomain = domainBoundaries.get(i);
            createArrayQuery.append(axisDomain.getFirst());
            createArrayQuery.append(":");
            createArrayQuery.append(axisDomain.getSecond());
            createArrayQuery.append(",");
            createArrayQuery.append(chunkSize);
            createArrayQuery.append(",");
            createArrayQuery.append('0');
        }

        createArrayQuery.append(']');

        executeTimedQuery(createArrayQuery.toString());

        String insertDataQuery = MessageFormat.format("LOAD {0} FROM ''{1}'' AS ''(char)''", benchContext.getArrayName(), filePath);
        long insertTime = executeTimedQuery(insertDataQuery, "-n");

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciDB_insert_results.csv");

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"", benchContext.getArrayName(), fileSize, chunkSize, noOfDimensions, insertTime));

    }

    @Override
    public void dropCollection() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", benchContext.getArrayName());
        executeTimedQuery(dropCollectionQuery);
    }
}
