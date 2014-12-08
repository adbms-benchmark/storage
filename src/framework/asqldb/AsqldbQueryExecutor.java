package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.Configuration;
import framework.ConnectionContext;
import framework.QueryExecutor;
import java.text.MessageFormat;
import java.util.List;
import org.asqldb.ras.RasUtil;
import org.asqldb.util.AsqldbConnection;
import org.asqldb.util.TimerUtil;
import rasj.RasGMArray;
import util.Pair;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbQueryExecutor extends QueryExecutor {

    private DataGenerator dataGenerator;
    private final DomainGenerator domainGenerator;
    private final AsqldbSystemController systemController;
    private final int noOfDimensions;

    public AsqldbQueryExecutor(ConnectionContext context, AsqldbSystemController systemController, int noOfDimensions) {
        super(context);
        this.domainGenerator = new DomainGenerator(noOfDimensions);
        this.systemController = systemController;
        this.noOfDimensions = noOfDimensions;
        AsqldbConnection.open(context.getUrl());
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        TimerUtil.startTimer("asqldb query");
        AsqldbConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("asqldb query");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(Configuration.COLLECTION_SIZE);
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        double approxChunkSize = Math.pow(Configuration.TILE_SIZE, 1 / ((double) noOfDimensions));
        int chunkSize = ((int) Math.ceil(approxChunkSize)) - 1;
        long tileSize = (long) Math.pow(chunkSize, noOfDimensions);

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        executeTimedQuery("CREATE TABLE " + Configuration.COLLECTION_NAME + " (a CHAR MDARRAY " + domainGenerator.getMDArrayDomain() + ")");

        // @TODO - support tiling and GMArray parameters in ASQLDB
        String rasqlColl = "PUBLIC_" + Configuration.COLLECTION_NAME + "_A";
        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [0:%d,0:%d] TILE SIZE %d",
                rasqlColl, chunkSize, chunkSize, tileSize);

        RasGMArray gmarray = AsqldbQueryGenerator.convertToRasGMArray(domainBoundaries, filePath);
        Integer oid = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(insertQuery, true, true, gmarray));

        AsqldbConnection.executeQuery("insert into " + Configuration.COLLECTION_NAME + " values (" + oid + ")");
        AsqldbConnection.commit();
    }

    @Override
    public void dropCollection() {
        String dropCollectionQuery = MessageFormat.format("DROP TABLE {0}", Configuration.COLLECTION_NAME);
        try {
            AsqldbConnection.executeQuery(dropCollectionQuery);
        } catch (Exception ex) {
        }
    }
}
