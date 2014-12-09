package framework.scidb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.QueryExecutor;
import framework.SystemController;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import util.Pair;

/**
 *
 * @author George Merticariu
 */
public class SciDBQueryExecutor extends QueryExecutor {
    private DomainGenerator domainGenerator;
    private DataGenerator dataGenerator;
    private int noOfDimensions;

    public SciDBQueryExecutor(ConnectionContext context, int noOfDimensions) {
        super(context);
        domainGenerator = new DomainGenerator(noOfDimensions);
        this.noOfDimensions = noOfDimensions;
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        List<String> commandList = new ArrayList<>();
        commandList.add("/opt/scidb/14.8/bin/iquery");
        commandList.add("-q");
        commandList.add(query);
        commandList.add("-p");
        commandList.add(String.valueOf(context.getPort()));
        Collections.addAll(commandList, args);

        long startTime = System.currentTimeMillis();
        SystemController.executeShellCommand(commandList.toArray(new String[]{}));
        long result = System.currentTimeMillis() - startTime;

        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(BenchmarkContext.COLLECTION_SIZE);
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        double approxChunkSize = Math.pow(BenchmarkContext.TILE_SIZE, 1 / ((double) noOfDimensions));
        int chunkSize = ((int) Math.ceil(approxChunkSize)) - 1;

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(BenchmarkContext.COLLECTION_NAME);
        createArrayQuery.append(" <");
        createArrayQuery.append(BenchmarkContext.COLLECTION_NAME);
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

        String insertDataQuery = MessageFormat.format("LOAD {0} FROM ''{1}'' AS ''(char)''", BenchmarkContext.COLLECTION_NAME, filePath);
        executeTimedQuery(insertDataQuery, "-n");
    }

    @Override
    public void dropCollection() {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", BenchmarkContext.COLLECTION_NAME);
        executeTimedQuery(dropCollectionQuery);
    }
}
