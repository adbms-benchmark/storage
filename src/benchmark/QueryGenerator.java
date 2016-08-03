package benchmark;

import data.QueryDomainGenerator;
import java.util.ArrayList;
import java.util.List;
import util.Pair;

/**
 * Generate benchmark sessions. Subclasses override it with details for specific
 * systems.
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public abstract class QueryGenerator {

    protected QueryDomainGenerator queryDomainGenerator;
    protected BenchmarkContext benchmarkContext;

    public QueryGenerator(BenchmarkContext benchmarkContext) {
        this.queryDomainGenerator = new QueryDomainGenerator(benchmarkContext);
        this.benchmarkContext = benchmarkContext;
    }

    public List<Pair<String, BenchmarkContext>> getCreateQueries() {
        List<Pair<String, BenchmarkContext>> ret = new ArrayList<>();

        List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchmarkContext);
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

    public Pair<String, BenchmarkContext> getCreateQuery(BenchmarkContext bc) {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }

    public Benchmark getBenchmark() {
        if (this.benchmarkContext.isSqlMdaBenchmark()) {
            return getSqlMdaBenchmark();
        } else if (this.benchmarkContext.isStorageBenchmark()) {
            return getStorageBenchmark();
        } else if (this.benchmarkContext.isCachingBenchmark()) {
            return getCachingBenchmark();
        } else {
            throw new RuntimeException("unknown benchmark type: " + this.benchmarkContext.getBenchmarkType());
        }
    }
    
    public Benchmark getStorageBenchmark() {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }

    public Benchmark getSqlMdaBenchmark() {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }

    public Benchmark getCachingBenchmark() {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }
}
