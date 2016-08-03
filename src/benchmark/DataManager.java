package benchmark;

import data.RandomDataGenerator;
import data.DomainGenerator;

/**
 * Load/drop data.
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 * @param <T> ADBMS system
 */
public abstract class DataManager<T> {
    
    protected final T systemController;
    protected final DomainGenerator domainGenerator;
    protected final BenchmarkContext benchmarkContext;
    protected final QueryExecutor<T> queryExecutor;
    protected RandomDataGenerator dataGenerator;

    public DataManager(T systemController, QueryExecutor<T> queryExecutor, BenchmarkContext benchmarkContext) {
        this.systemController = systemController;
        this.benchmarkContext = benchmarkContext;
        this.domainGenerator = new DomainGenerator(benchmarkContext.getArrayDimensionality());
        this.queryExecutor = queryExecutor;
    }

    /**
     * Load benchmark data.
     * 
     * @return the time in ms
     */
    public abstract long loadData() throws Exception;

    /**
     * Drop benchmark data.
     * 
     * @return the time in ms
     */
    public abstract long dropData() throws Exception;

    /**
     * Generate benchmark data.
     * 
     * @return the time in ms
     */
    public void generateData() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
