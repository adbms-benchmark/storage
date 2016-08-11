package system.scidb;

import benchmark.caching.CachingBenchmarkDataManager;
import benchmark.QueryExecutor;
import benchmark.BenchmarkContext;
import java.text.MessageFormat;
import java.util.List;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBCachingBenchmarkDataManager extends CachingBenchmarkDataManager<SciDBSystem> {

    public SciDBCachingBenchmarkDataManager(SciDBSystem systemController, 
            QueryExecutor<SciDBSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public long loadData() throws Exception {
        long totalTime = 0;
        
        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            
            String createArray = String.format("CREATE ARRAY %s <v%d:float> [ d1=0:%d,%d,0, d2=0:%d,%d,0 ]",
                    arrayName, i, BAND_WIDTH - 1, TILE_WIDTH, BAND_HEIGHT - 1, TILE_HEIGHT);
            queryExecutor.executeTimedQuery(createArray);
            String insertDataQuery = MessageFormat.format("INSERT( INPUT({0}, ''{1}'', 0, ''(float)''), {0});",
                    arrayName, sliceFilePaths.get(i));

            totalTime += queryExecutor.executeTimedQuery(insertDataQuery, "-n", "-a");
        }
        
        return totalTime;
    }

    @Override
    public long dropData() throws Exception {
        long totalTime = 0;
        
        for (int i = 0; i < ARRAY_NO; i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", arrayName);
            totalTime += queryExecutor.executeTimedQuery(dropCollectionQuery);
        }
        
        return totalTime;
    }
}
