package framework;

import data.DataGenerator;
import data.DomainGenerator;
import framework.context.BenchmarkContext;

/**
 * @TODO - merge with AdbmsSystem
 *
 * @author George Merticariu
 */
public abstract class QueryExecutor<T> {
    
    protected DomainGenerator domainGenerator;
    protected DataGenerator dataGenerator;
    protected int noOfDimensions;
    protected BenchmarkContext benchContext;
    protected T context;

    protected QueryExecutor(T context, BenchmarkContext benchmarkContext) {
        this.context = context;
        this.benchContext = benchmarkContext;
        this.noOfDimensions = benchmarkContext.getArrayDimensionality();
        this.domainGenerator = new DomainGenerator(noOfDimensions);
    }

    public abstract long executeTimedQuery(String query, String... args) throws Exception;

    public abstract void createCollection() throws Exception;

    public abstract void dropCollection() throws Exception;

    protected String report(String systemName, String query, int dataSize, long time) {
        return systemName + ",\"" + query + "\","
                + dataSize + "," + time;
    }

}
