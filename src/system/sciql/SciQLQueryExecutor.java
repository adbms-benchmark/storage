package system.sciql;

import benchmark.QueryExecutor;
import benchmark.BenchmarkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StopWatch;

/**
 * @author Dimitar Misev
 */
public class SciQLQueryExecutor extends QueryExecutor<SciQLSystem> {

    private static final Logger log = LoggerFactory.getLogger(SciQLQueryExecutor.class);

    public SciQLQueryExecutor(BenchmarkContext benchContext, SciQLSystem systemController) {
        super(systemController, benchContext);
    }

    @Override
    public long executeTimedQuery(String query, String... args) throws Exception {
        StopWatch timer = new StopWatch();
        SciQLConnection.executeQuery(query);
        return timer.getElapsedTime();
    }

    public long executeTimedQueryUpdate(String query) {
        StopWatch timer = new StopWatch();
        SciQLConnection.executeUpdateQuery(query);
        return timer.getElapsedTime();
    }
}
