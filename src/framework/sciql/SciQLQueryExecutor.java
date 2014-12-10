package framework.sciql;

import data.DataGenerator;
import data.DomainGenerator;
import framework.BenchmarkContext;
import framework.ConnectionContext;
import framework.QueryExecutor;
import java.text.MessageFormat;
import java.util.List;
import org.asqldb.util.TimerUtil;
import util.Pair;

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
        TimerUtil.startTimer("sciql query");
        SciQLConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());

        double approxChunkSize = Math.pow(benchContext.getCollTileSize(), 1 / ((double) noOfDimensions));
        int chunkSize = ((int) Math.ceil(approxChunkSize)) - 1;
        long tileSize = (long) Math.pow(chunkSize, noOfDimensions);

        String createArray = "CREATE ARRAY " + benchContext.getCollName() + domainGenerator.getSciQLDomain(domainBoundaries);
        executeTimedQuery(createArray);

        
    }

    @Override
    public void dropCollection() {
        String dropCollectionQuery = MessageFormat.format("DROP ARRAY {0}", benchContext.getCollName());
        try {
            SciQLConnection.executeQuery(dropCollectionQuery);
        } catch (Exception ex) {
        }
    }
}
