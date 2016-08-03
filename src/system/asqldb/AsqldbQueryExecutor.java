package system.asqldb;

import benchmark.QueryExecutor;
import benchmark.BenchmarkContext;
import java.io.InputStream;
import org.asqldb.util.AsqldbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StopWatch;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbQueryExecutor extends QueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(AsqldbQueryExecutor.class);

    public AsqldbQueryExecutor(AsqldbSystem systemController, BenchmarkContext benchContext) {
        super(systemController, benchContext);
        AsqldbConnection.open(systemController.getUrl());
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        StopWatch timer = new StopWatch();
        AsqldbConnection.executeQuery(query);
        return timer.getElapsedTime();
    }

    @Override
    public long executeTimedQueryUpdate(String query, InputStream in) {
        StopWatch timer = new StopWatch();
        AsqldbConnection.executeUpdateQuery(query, in);
        return timer.getElapsedTime();
    }

}
