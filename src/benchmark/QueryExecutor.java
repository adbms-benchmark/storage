package benchmark;

import data.RandomDataGenerator;
import data.DomainGenerator;
import java.io.InputStream;

/**
 * Provide functionality for executing array queries, creating/dropping a collection/table.
 * 
 * @author George Merticariu
 * @param <T> ADBMS system
 */
public abstract class QueryExecutor<T> {
    
    protected T systemController;
    protected DomainGenerator domainGenerator;
    protected RandomDataGenerator dataGenerator;
    protected BenchmarkContext benchmarkContext;

    protected QueryExecutor(T systemController, BenchmarkContext benchmarkContext) {
        this.systemController = systemController;
        this.benchmarkContext = benchmarkContext;
        this.domainGenerator = new DomainGenerator(benchmarkContext.getArrayDimensionality());
    }

    /**
     * Execute a query with given arguments.
     * 
     * @param query particular query
     * @param args further command arguments
     * @return the time in ms
     */
    public abstract long executeTimedQuery(String query, String... args) throws Exception;
    
    /**
     * Execute an update query. The InputStream in can be null.
     * 
     * @param query the query to execute
     * @param in input stream of the data
     * @return the execution time in ms
     */
    public long executeTimedQueryUpdate(String query, InputStream in) {
        throw new UnsupportedOperationException("must be implemented by the subclass");
    }

    protected String report(String systemName, String query, int dataSize, long time) {
        return systemName + ",\"" + query + "\"," + dataSize + "," + time;
    }

}
