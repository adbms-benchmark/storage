package system.scidb;

import benchmark.caching.CachingBenchmarkDataManager;
import benchmark.QueryExecutor;
import benchmark.BenchmarkContext;
import java.text.MessageFormat;
import java.util.List;
import util.DomainUtil;

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
            
            long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize());
            String createArray = String.format("CREATE ARRAY %s <v%d:float> [ d1=0:%d,%d,0, d2=0:%d,%d,0 ]",
                    arrayName, i, BAND_WIDTH - 1, tileUpperBound + 1, BAND_HEIGHT - 1, tileUpperBound + 1);
            queryExecutor.executeTimedQuery(createArray);
            String insertDataQuery = MessageFormat.format("LOAD({0}, ''{1}'', 0, ''(float)'');",
                    arrayName, sliceFilePaths.get(i));

            totalTime += queryExecutor.executeTimedQuery(insertDataQuery);
        }
        
        return totalTime;
    }

    @Override
    public long dropData() throws Exception {
        long totalTime = 0;
        
        for (int i = 0; i < ARRAY_NO; i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            String dropCollectionQuery = MessageFormat.format("remove({0});", arrayName);
            totalTime += queryExecutor.executeTimedQuery(dropCollectionQuery);
        }
        
        return totalTime;
    }
}
