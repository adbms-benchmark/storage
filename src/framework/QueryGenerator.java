package framework;

import data.BenchmarkQuery;
import data.QueryDomainGenerator;
import framework.context.BenchmarkContext;
import framework.context.BenchmarkContextGenerator;
import framework.context.BenchmarkContextJoin;
import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryGenerator {

    protected int noOfDimensions;
    protected QueryDomainGenerator queryDomainGenerator;
    protected BenchmarkContext benchContext;

    public QueryGenerator(BenchmarkContext benchmarkContext) {
        this.queryDomainGenerator = new QueryDomainGenerator(benchmarkContext);
        this.benchContext = benchmarkContext;
    }

    public abstract List<BenchmarkQuery> getBenchmarkQueries();

    public abstract BenchmarkQuery getMiddlePointQuery();

    public List<Pair<String, BenchmarkContext>> getCreateQueries() {
        List<Pair<String, BenchmarkContext>> ret = new ArrayList<>();

        List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchContext);
        for (BenchmarkContext bc : benchContexts) {
            if (bc instanceof BenchmarkContextJoin) {
                BenchmarkContext[] joinedContexts = ((BenchmarkContextJoin) bc).getBenchmarkContexts();
                for (BenchmarkContext joinedContext : joinedContexts) {
                    ret.add(getCreateQuery(joinedContext));
                }
            } else {
                ret.add(getCreateQuery(bc));
            }
        }
        return ret;
    }

    public List<BenchmarkQuery> getSqlMdaBenchmarkQueries() {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }

    public Pair<String, BenchmarkContext> getCreateQuery(BenchmarkContext bc) {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }
}
