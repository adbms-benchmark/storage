package framework.scidb;

import framework.rasdaman.*;
import data.DataGenerator;
import framework.DataManager;
import framework.QueryExecutor;
import framework.context.BenchmarkContext;
import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import util.DomainUtil;
import util.IO;
import util.Pair;
import util.StopWatch;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBStorageBenchmarkDataManager extends DataManager<SciDBSystem> {

    public SciDBStorageBenchmarkDataManager(SciDBSystem systemController, 
            QueryExecutor<SciDBSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public long dropData() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", benchmarkContext.getArrayName());
        return queryExecutor.executeTimedQuery(dropCollectionQuery);
    }

    @Override
    public long loadData() throws Exception {
        StopWatch timer = new StopWatch();
        loadStorageBenchmarkData();
        return timer.getElapsedTime();
    }
    
    private void loadStorageBenchmarkData() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        long chunkUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getCollTileSize());
        long chunkSize = chunkUpperBound + 1l;
        dataGenerator = new DataGenerator(fileSize, benchmarkContext.getDataDir());
        String filePath = dataGenerator.getFilePath();

        StringBuilder createArrayQuery = new StringBuilder();
        createArrayQuery.append("CREATE ARRAY ");
        createArrayQuery.append(benchmarkContext.getArrayName());
        createArrayQuery.append(" <");
        createArrayQuery.append(benchmarkContext.getArrayName());
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

        queryExecutor.executeTimedQuery(createArrayQuery.toString());

        String insertDataQuery = MessageFormat.format("LOAD {0} FROM ''{1}'' AS ''(char)''", benchmarkContext.getArrayName(), filePath);
        long insertTime = queryExecutor.executeTimedQuery(insertDataQuery, "-n");

        File resultsDir = IO.getResultsDir();
        File insertResultFile = new File(resultsDir.getAbsolutePath(), "SciDB_insert_results.csv");

        IO.appendLineToFile(insertResultFile.getAbsolutePath(), String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"", 
                benchmarkContext.getArrayName(), fileSize, chunkSize, benchmarkContext.getArrayDimensionality(), insertTime));
    }
}
