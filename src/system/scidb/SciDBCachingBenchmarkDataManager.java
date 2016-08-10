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
        
        String attributes = SciDBAFLQueryGenerator.getAttributes(BAND_NO, "uint16");
        createColl(attributes);
        
        List<String> sliceFilePaths = getSliceFilePaths(benchmarkContext);
        for (int i = 0; i < sliceFilePaths.size(); i++) {
            String insertDataQuery = MessageFormat.format(
                      "INSERT("
                    + " REDIMENSION("
                    + "  APPLY("
                    + "   INPUT(<{3}>[a=0:7999,500,0, b=0:7999,500,0],"
                    + "         ''{1}'', 0, "
                    + "         ''(uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16)''),"
                    + "   d0, {2}, d1, a, d2, b),"
                    + "  {0}),"
                    + " {0}"
                    + ");",
                    benchmarkContext.getArrayName(), sliceFilePaths.get(i), i, attributes);

            totalTime += queryExecutor.executeTimedQuery(insertDataQuery, "-n", "-a");
        }
        
        return totalTime;
    }
    
    private void createColl(String attributes) throws Exception {
        String createQuery = String.format("CREATE ARRAY %s <%s> [ d0=0:%d,1,0, d1=0:7999,500,0, d2=0:7999,500,0 ]", 
                benchmarkContext.getArrayName(), attributes, MAX_SLICE_NO);
        queryExecutor.executeTimedQuery(createQuery);
    }

    @Override
    public long dropData() throws Exception {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", benchmarkContext.getArrayName());
        return queryExecutor.executeTimedQuery(dropCollectionQuery);
    }
    
}
