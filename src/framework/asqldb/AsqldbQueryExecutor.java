package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import framework.context.BenchmarkContextGenerator;
import framework.context.BenchmarkContextJoin;
import framework.context.SystemContext;
import framework.rasdaman.RasdamanQueryGenerator;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import org.asqldb.ras.RasUtil;
import org.asqldb.util.AsqldbConnection;
import org.asqldb.util.TimerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.BenchmarkUtil;
import util.IO;
import util.Pair;
import util.ProcessExecutor;
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
