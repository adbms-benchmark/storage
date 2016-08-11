package system.rasdaman;

import benchmark.caching.CachingBenchmarkDataManager;
import benchmark.BenchmarkContext;
import benchmark.caching.CachingBenchmarkContext;
import java.text.MessageFormat;
import java.util.List;
import util.Pair;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanCachingBenchmarkDataManager extends CachingBenchmarkDataManager<RasdamanSystem> {
    
    public RasdamanCachingBenchmarkDataManager(RasdamanSystem systemController,
            RasdamanQueryExecutor queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }

    @Override
    public long loadData() throws Exception {
        long totalTime = 0;
        
        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String arrayName = benchmarkContext.getArrayNameN(i);
            
            queryExecutor.executeTimedQuery(String.format("CREATE COLLECTION %s FloatSet", arrayName));
            
            String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:%d,0:%d] TILE SIZE %d",
                    arrayName, TILE_WIDTH - 1, TILE_HEIGHT - 1, TILE_SIZE);
            String mddDomain = String.format("[0:%d,0:%d]", BAND_WIDTH - 1, BAND_HEIGHT - 1);
            totalTime += queryExecutor.executeTimedQuery(insertQuery,
                    "-f", sliceFilePaths.get(i),
                    "--mdddomain", mddDomain,
                    "--mddtype", "FloatImage"
            );
        }

        return totalTime;
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
