package framework;

import data.BenchmarkQuery;
import data.QueryDomainGenerator;
import framework.context.BenchmarkContext;
import util.Pair;

import java.util.List;
import java.util.Map;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryGenerator {

    protected int noOfDimensions;
    protected QueryDomainGenerator queryDomainGenerator;
    protected BenchmarkContext benchContext;

    public QueryGenerator(BenchmarkContext benchmarkContext) {
        this.queryDomainGenerator = new QueryDomainGenerator(benchContext);
        this.benchContext = benchmarkContext;
    }

    public abstract List<BenchmarkQuery> getBenchmarkQueries();

    public abstract BenchmarkQuery getMiddlePointQuery();
}