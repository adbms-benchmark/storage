package system.rasdaman;

import benchmark.BenchmarkContext;
import benchmark.operations.OperationsBenchmarkContext;
import benchmark.operations.OperationsBenchmarkDataManager;
import data.RandomDataGenerator;
import util.Pair;
import util.StopWatch;

import java.text.MessageFormat;
import java.util.List;

/**
 * Created by Danut Rusu on 23.03.17.
 */
public class RasdamanOperationsBenchmarkDataManager extends OperationsBenchmarkDataManager<RasdamanSystem> {

    private final RasdamanTypeManager typeManager;


    public RasdamanOperationsBenchmarkDataManager(RasdamanSystem systemController,
                                               RasdamanQueryExecutor queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
        typeManager = new RasdamanTypeManager(queryExecutor);
    }

    @Override
    public long loadData() throws Exception {
        StopWatch timer = new StopWatch();
        loadOperationsBenchmarkData();
        return timer.getElapsedTime();
    }

    private void loadOperationsBenchmarkData() throws Exception {
        long insertTime;
        String dataType = ((OperationsBenchmarkContext) benchmarkContext).getDataType();

        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchmarkContext.getArraySize(), dataType);
        long fileSize = domainGenerator.getFileSize(domainBoundaries, dataType);

        String tileBoundaries = domainGenerator.getTileDomainBoundariesOperations();


        System.out.println(fileSize);

        long tileSize = ( benchmarkContext.getArraySize() * domainGenerator.getBytes(dataType))  / 100;

        dataGenerator = new RandomDataGenerator(fileSize, benchmarkContext.getDataDir());
        String filePath = dataGenerator.getFilePath();


        Pair<String, String> aChar = typeManager.createOperationsType(benchmarkContext.getArrayDimensionality(), dataType);

        String createCollectionQuery = String.format("CREATE COLLECTION %s %s", benchmarkContext.getArrayName(), aChar.getSecond());
        queryExecutor.executeTimedQuery(createCollectionQuery);

        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING ALIGNED %s TILE SIZE %d",
                benchmarkContext.getArrayName(), tileBoundaries, tileSize);
        System.out.println("Executing insert query: " + insertQuery);
        insertTime = queryExecutor.executeTimedQuery(insertQuery,
                "--mddtype", aChar.getFirst(),
                "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                "--file", filePath);
    }

    @Override
    public long dropData() throws Exception {
        long ret = 0;
        for (int i = 0; i < ARRAY_NO; i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            String dropQuery = MessageFormat.format("DROP COLLECTION {0}", arrayName);
            ret += queryExecutor.executeTimedQuery(dropQuery);
        }
        return ret;
    }
}
