package framework.asqldb;

import framework.DataManager;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import framework.rasdaman.RasdamanSystem;
import framework.sciql.SciQLSqlMdaBenchmarkDataManager;
import java.io.IOException;
import org.asqldb.util.AsqldbConnection;

/**
 * ASQLDB system manager.
 *
 * @author Dimitar Misev
 */
public class AsqldbSystem extends RasdamanSystem {

    public AsqldbSystem(String propertiesPath) throws IOException {
        super(propertiesPath);
        systemName = "ASQLDB";
    }

    @Override
    public void restartSystem() throws Exception {
        super.restartSystem();
        AsqldbConnection.close();
        // nop
        AsqldbConnection.open(getUrl());
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new AsqldbQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new AsqldbQueryExecutor(this, benchmarkContext);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isSqlMdaBenchmark()) {
            return new AsqldbSqlMdaBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }

}
