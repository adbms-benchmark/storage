package framework;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryExecutor<T> {

    protected T context;

    protected QueryExecutor(T context) {
        this.context = context;
    }

    public abstract long executeTimedQuery(String query, String... args) throws Exception;

    public abstract void createCollection() throws Exception;

    public abstract void dropCollection() throws Exception;

    protected String report(String systemName, String query, int dataSize, long time) {
        return systemName + ",\"" + query + "\","
                + dataSize + "," + time;
    }

}
