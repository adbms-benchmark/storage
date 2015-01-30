package framework.scidb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.context.BenchmarkContext;
import framework.QueryExecutor;
import framework.SystemController;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import framework.context.SciDBContext;
import util.DomainUtil;
import util.IO;
import util.Pair;

/**
 * @author George Merticariu
 */
public class SciDBQueryExecutor extends QueryExecutor<SciDBContext> {
    private DomainGenerator domainGenerator;
    private DataGenerator dataGenerator;
    private int noOfDimensions;
    private BenchmarkContext benchContext;

    public SciDBQueryExecutor(SciDBContext context, BenchmarkContext benchContext, int noOfDimensions) {
        super(context);
        domainGenerator = new DomainGenerator(noOfDimensions);
        this.noOfDimensions = noOfDimensions;
        this.benchContext = benchContext;
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        List<String> commandList = new ArrayList<>();
        commandList.add(context.getExecuteQueryBin());
        commandList.add("-q");
        commandList.add(query);
        commandList.add("-p");
        commandList.add(String.valueOf(context.getPort()));
        Collections.addAll(commandList, args);

        long startTime = System.currentTimeMillis();
        int status = SystemController.executeShellCommand(commandList.toArray(new String[]{}));
        long result = System.currentTimeMillis() - startTime;

        if (status != 0) {
            throw new Exception(String.format("Query execution failed with status %d", status));
        }

        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        long chunkUpperBound = DomainUtil.getDimensionUpperBound(noOfDimensions, benchContext.getCollTileSize());
        long chunkSize = chunkUpperBound + 1l;
        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(benchContext.getCollName1());
        createArrayQuery.append(" <");
        createArrayQuery.append(benchContext.getCollName1());
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

        String insertDataQuery = MessageFormat.format("LOAD {0} FROM ''{1}'' AS ''(char)''", benchContext.getCollName1(), filePath);
        long insertTime = executeTimedQuery(insertDataQuery, "-n");

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciDB_insert_results.csv");

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"", benchContext.getCollName1(), fileSize, chunkSize, noOfDimensions, insertTime));

    }

    @Override
    public void dropCollection() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", benchContext.getCollName1());
        executeTimedQuery(dropCollectionQuery);
    }
}
