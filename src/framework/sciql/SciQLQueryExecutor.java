package framework.sciql;

import data.DataGenerator;
import data.DomainGenerator;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.QueryExecutor;
import org.asqldb.util.TimerUtil;

/**
 *
 * @author Dimitar Misev
 */
public class SciQLQueryExecutor extends QueryExecutor {

    private DataGenerator dataGenerator;
    private final DomainGenerator domainGenerator;
    private final SciQLSystemController systemController;
    private final BenchmarkContext benchContext;
    private final int noOfDimensions;

    public SciQLQueryExecutor(ConnectionContext context, SciQLSystemController systemController,
            BenchmarkContext benchContext, int noOfDimensions) {
        super(context);
        this.domainGenerator = new DomainGenerator(noOfDimensions);
        this.systemController = systemController;
        this.benchContext = benchContext;
        this.noOfDimensions = noOfDimensions;
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("sciql query");
        SciQLConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query");
        System.out.println("time: " + result + " ms");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        try {
            SciQLConnection.executeUpdateQuery("CALL rs.attach('" + benchContext.getDataFile() + "')");
            SciQLConnection.executeUpdateQuery("CALL rs.import(1)");
        } catch (Exception ex) {
            dropCollection();
            throw ex;
        } finally {
        }
    }

    @Override
    public void dropCollection() {
        try {
            SciQLConnection.executeUpdateQuery("DELETE FROM rs.files");
            SciQLConnection.executeUpdateQuery("DELETE FROM rs.catalog");
            SciQLConnection.executeUpdateQuery("DROP ARRAY rs.image1");
        } catch (Exception ex) {
        }
    }
}
