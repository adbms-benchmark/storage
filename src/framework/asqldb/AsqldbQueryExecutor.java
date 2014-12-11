package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.QueryExecutor;
import java.io.FileInputStream;
import java.io.InputStream;
import org.asqldb.util.AsqldbConnection;
import org.asqldb.util.TimerUtil;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbQueryExecutor extends QueryExecutor {

    public static final String TMP_TIFF_FILE = "/tmp/tiff2d.tif";

    private DataGenerator dataGenerator;
    private final DomainGenerator domainGenerator;
    private final AsqldbSystemController systemController;
    private final BenchmarkContext benchContext;
    private final int noOfDimensions;

    public AsqldbQueryExecutor(ConnectionContext context, AsqldbSystemController systemController,
            BenchmarkContext benchContext, int noOfDimensions) {
        super(context);
        this.domainGenerator = new DomainGenerator(noOfDimensions);
        this.systemController = systemController;
        this.benchContext = benchContext;
        this.noOfDimensions = noOfDimensions;
        AsqldbConnection.open(context.getUrl());
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("asqldb query");
        AsqldbConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("asqldb query");
        System.out.println("time: " + result + " ms");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        try {
            executeTimedQuery("CREATE TABLE " + benchContext.getCollName() + " (a CHAR MDARRAY " + domainGenerator.getMDArrayDomain() + ")");
            InputStream fin = new FileInputStream(benchContext.getDataFile());
            AsqldbConnection.executeUpdateQuery("insert into " + benchContext.getCollName() + " values (mdarray_decode(?))", fin);
            AsqldbConnection.commit();
        } catch (Exception ex) {
            dropCollection();
            throw ex;
        }
    }

    @Override
    public void dropCollection() {
        try {
            AsqldbConnection.executeQuery("DROP TABLE " + benchContext.getCollName());
            AsqldbConnection.commit();
        } catch (Exception ex) {
        }
    }
}
