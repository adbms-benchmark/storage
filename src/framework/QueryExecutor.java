package framework;

/**
 *
 * @author George Merticariu
 */
public abstract class QueryExecutor {

    protected Context context;

    protected QueryExecutor(Context context) {
        this.context = context;
    }

    public abstract long executeTimedQuery(String query, String... args);

    public abstract void createCollection() throws Exception;

    public abstract void dropCollection();

}
