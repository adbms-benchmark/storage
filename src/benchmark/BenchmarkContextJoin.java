package benchmark;

/**
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class BenchmarkContextJoin extends BenchmarkContext {
    private final BenchmarkContext[] benchmarkContexts;

    public BenchmarkContextJoin(BenchmarkContext... benchmarkContexts) {
        super(0, 0, 0, 0, "", 0);
        this.benchmarkContexts = benchmarkContexts;
    }

    public BenchmarkContext[] getBenchmarkContexts() {
        return benchmarkContexts;
    }

}
