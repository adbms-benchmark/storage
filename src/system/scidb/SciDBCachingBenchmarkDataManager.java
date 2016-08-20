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
    
    private static final int TYPE_SIZE = 8;
    private static final String TYPE_BASE = "double";

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
            
            long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize() / TYPE_SIZE);
            String createArray = String.format("CREATE ARRAY %s <v%d:%s> [ d1=0:%d,%d,0, d2=0:%d,%d,0 ]",
                    arrayName, i, TYPE_BASE, BAND_WIDTH - 1, tileUpperBound, BAND_HEIGHT - 1, tileUpperBound);
            queryExecutor.executeTimedQuery(createArray);
            String insertDataQuery = MessageFormat.format("LOAD({0}, ''{1}'', 0, ''({2})'');",
                    arrayName, sliceFilePaths.get(i), TYPE_BASE);

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
