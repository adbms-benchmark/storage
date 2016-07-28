package framework.scidb;

import framework.rasdaman.*;
import framework.DataManager;
import framework.QueryExecutor;
import framework.context.BenchmarkContext;

/**
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBCachingBenchmarkDataManager extends DataManager<SciDBSystem> {

    public SciDBCachingBenchmarkDataManager(SciDBSystem systemController, 
            QueryExecutor<SciDBSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
    }


    @Override
    public long loadData() throws Exception {
        return 0;
    }

    @Override
    public long dropData() throws Exception {
        return 0;
    }
    
}
