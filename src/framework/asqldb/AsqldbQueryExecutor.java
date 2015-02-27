package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import framework.AdbmsSystem;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import framework.context.BenchmarkContextGenerator;
import framework.context.BenchmarkContextJoin;
import framework.context.SystemContext;
import framework.rasdaman.RasdamanQueryGenerator;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import org.asqldb.ras.RasUtil;
import org.asqldb.util.AsqldbConnection;
import org.asqldb.util.TimerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.BenchmarkUtil;
import util.IO;
import util.Pair;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbQueryExecutor extends QueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(AsqldbQueryExecutor.class);

    private final AsqldbSystem systemController;

    public AsqldbQueryExecutor(SystemContext context,
            BenchmarkContext benchContext, AsqldbSystem systemController) {
        super(context, benchContext);
        this.systemController = systemController;
        AsqldbConnection.open(context.getUrl());
    }

    @Override
    public long executeTimedQuery(String query, String... args) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("asqldb query");
        AsqldbConnection.executeQuery(query);
        long result = TimerUtil.getElapsedMilli("asqldb query");
        TimerUtil.clearTimers();
        System.out.println("time: " + result + " ms");
        return result;
    }

    public long executeTimedQueryUpdate(String query, InputStream in) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("asqldb query update");
        AsqldbConnection.executeUpdateQuery(query, in);
        long result = TimerUtil.getElapsedMilli("asqldb query update");
        TimerUtil.clearTimers();
        System.out.println("time: " + result + " ms");
        return result;
    }

    @Override
    public void createCollection() throws Exception {

        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchContext);
        List<Pair<String, BenchmarkContext>> createQueries = queryGenerator.getCreateQueries();

        for (Pair<String, BenchmarkContext> createQuery : createQueries) {
            BenchmarkContext bc = createQuery.getSecond();
            noOfDimensions = bc.getArrayDimensionality();
            String asqldbTableName = bc.getArrayName();
            String rasCollName = BenchmarkUtil.getAsqldbCollectionNameInRasdaman(asqldbTableName, "v");
            if (RasUtil.collectionExists(rasCollName)) {
                log.info("Array " + asqldbTableName + " found, not reingesting.");
                return;
            }
            AsqldbConnection.executeUpdateQuery(createQuery.getFirst());

            String insertQuery = "insert into " + rasCollName + " values $1";
            Pair<String, String> rasType = systemController.createRasdamanType(noOfDimensions, "char");
            List<Pair<Long, Long>> domainBoundaries = new DomainGenerator(
                    bc.getArrayDimensionality()).getDomainBoundaries(bc.getArraySize());
            dataGenerator = new DataGenerator(bc.getArraySize(), bc.getDataDir());
            String filePath = dataGenerator.getFilePath();

            TimerUtil.clearTimers();
            TimerUtil.startTimer("insert");
            AdbmsSystem.executeShellCommand(
                    systemController.getQueryCommand(),
                    "-q", insertQuery,
                    "--user", "rasadmin",
                    "--passwd", "rasadmin",
                    "--mddtype", rasType.getFirst(),
                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                    "-f", filePath);
            Integer oid = ((Double) RasUtil.head(RasUtil.executeRasqlQuery(
                    "select oid(c) from " + rasCollName + " as c", true, true))).intValue();
            AsqldbConnection.executeQuery("insert into " + asqldbTableName + " values (" + oid + ");");
            AsqldbConnection.commit();
            long insertTime = TimerUtil.getElapsedMilli("insert");

            File resultsDir = IO.getResultsDir();
            File insertResultFile = new File(resultsDir.getAbsolutePath(), "asqldb_insert_results.csv");
            IO.appendLineToFile(insertResultFile.getAbsolutePath(),
                    String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
                            asqldbTableName, bc.getArraySize(), -1, noOfDimensions, insertTime));
        }
    }

    @Override
    public void dropCollection() {
        try {
            if (benchContext.isSqlMdaBenchmark()) {
                List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchContext);
                for (BenchmarkContext bc : benchContexts) {
                    if (bc instanceof BenchmarkContextJoin) {
                        BenchmarkContext[] joinedContexts = ((BenchmarkContextJoin) bc).getBenchmarkContexts();
                        for (BenchmarkContext joinedContext : joinedContexts) {
                            AsqldbConnection.executeUpdateQuery("DROP TABLE " + joinedContext.getArrayName());
                        }
                    } else {
                        AsqldbConnection.executeUpdateQuery("DROP TABLE " + bc.getArrayName());
                    }
                }
                AsqldbConnection.commit();
            }
        } catch (Exception ex) {
        }
    }
}
