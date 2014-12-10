package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.BenchmarkContext;
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
        TimerUtil.startTimer("asqldb query");
        AsqldbConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("asqldb query");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        List<Pair<Long, Long>> domainBoundaries = domainGenerator.getDomainBoundaries(benchContext.getCollSize());
        long fileSize = domainGenerator.getFileSize(domainBoundaries);

        double approxChunkSize = Math.pow(benchContext.getCollTileSize(), 1 / ((double) noOfDimensions));
        int chunkSize = ((int) Math.ceil(approxChunkSize)) - 1;
        long tileSize = (long) Math.pow(chunkSize, noOfDimensions);

        dataGenerator = new DataGenerator(fileSize);
        String filePath = dataGenerator.getFilePath();

        executeTimedQuery("CREATE TABLE " + benchContext.getCollName() + " (a CHAR MDARRAY " + domainGenerator.getMDArrayDomain() + ")");

        // @TODO - support tiling and GMArray parameters in ASQLDB
        String rasqlColl = "PUBLIC_" + benchContext.getCollName() + "_A";
        String tilingDomain = "";
        for (int i = 0; i < noOfDimensions; i++) {
            if (i > 0) {
                tilingDomain += ",";
            }
            tilingDomain += "0:" + chunkSize;
        }
        String insertQuery = String.format("INSERT INTO %s VALUES $1 TILING REGULAR [%s] TILE SIZE %d",
                rasqlColl, tilingDomain, tileSize);

        RasGMArray gmarray = AsqldbQueryGenerator.convertToRasGMArray(domainBoundaries, filePath);
        Integer oid = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(insertQuery, true, true, gmarray));

        AsqldbConnection.executeQuery("insert into " + benchContext.getCollName() + " values (" + oid + ")");
        AsqldbConnection.commit();
    }

    @Override
    public void dropCollection() {
        String dropCollectionQuery = MessageFormat.format("DROP TABLE {0}", benchContext.getCollName());
        try {
            AsqldbConnection.executeQuery(dropCollectionQuery);
        } catch (Throwable ex) {
        }
    }
}
