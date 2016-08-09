package benchmark.sqlmda;

import benchmark.BenchmarkContext;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContextJoin extends BenchmarkContext {
    private final BenchmarkContext[] benchmarkContexts;

    public BenchmarkContextJoin(BenchmarkContext... benchmarkContexts) {
        super(0, "", 0, TYPE_SQLMDA);
        this.benchmarkContexts = benchmarkContexts;
    }

    public BenchmarkContext[] getBenchmarkContexts() {
        return benchmarkContexts;
    }

}
