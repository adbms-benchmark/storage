package system.rasdaman;

import benchmark.caching.CachingBenchmarkDataManager;
import benchmark.BenchmarkContext;
import benchmark.caching.CachingBenchmarkContext;
import java.text.MessageFormat;
import java.util.List;
import util.DomainUtil;
import util.Pair;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class RasdamanCachingBenchmarkDataManager extends CachingBenchmarkDataManager<RasdamanSystem> {
    
    private static final int TYPE_SIZE = 8;
    private static final String TYPE_MDD = "DoubleImage";
    private static final String TYPE_SET = "DoubleSet";
    
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
            
            queryExecutor.executeTimedQuery(String.format("CREATE COLLECTION %s %s", arrayName, TYPE_SET));
            
            long tileUpperBound = DomainUtil.getDimensionUpperBound(benchmarkContext.getArrayDimensionality(), benchmarkContext.getTileSize() / TYPE_SIZE);
            String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:%d,0:%d] TILE SIZE %d",
                    arrayName, tileUpperBound - 1, tileUpperBound - 1, tileUpperBound * tileUpperBound * TYPE_SIZE);
            String mddDomain = String.format("[0:%d,0:%d]", BAND_WIDTH - 1, BAND_HEIGHT - 1);
            totalTime += queryExecutor.executeTimedQuery(insertQuery,
                    "-f", sliceFilePaths.get(i),
                    "--mdddomain", mddDomain,
                    "--mddtype", TYPE_MDD
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
