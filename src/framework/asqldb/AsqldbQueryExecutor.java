package framework.asqldb;

import data.DataGenerator;
import data.DomainGenerator;
import static util.IO.HOME_DIR;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.QueryExecutor;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import org.asqldb.util.AsqldbConnection;
import org.asqldb.util.TimerUtil;
import util.IO;

/**
 *
 * @author Dimitar Misev
 */
public class AsqldbQueryExecutor extends QueryExecutor {

    public static final String TMP_TIFF_FILE = "/tmp/tiff2d.tif";

    private DataGenerator dataGenerator;
    private final DomainGenerator domainGenerator;
    private final AsqldbSystem systemController;
    private final BenchmarkContext benchContext;
    private final int noOfDimensions;

    public AsqldbQueryExecutor(ConnectionContext context, AsqldbSystem systemController,
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
        String benchmarkFilename = HOME_DIR + "/benchmark_insert.csv";
        IO.deleteFile(benchmarkFilename);

        try (PrintWriter pr = new PrintWriter(new FileWriter(benchmarkFilename, true))) {

            pr.println("system_name,query,data_size,time_in_ms");
            pr.flush();
//            for (TableContext tableContext : BenchmarkContext.dataSizes) {
//                executeTimedQuery("CREATE TABLE " + tableContext.asqldbTable1 + " (a CHAR MDARRAY [x,y])");
//                executeTimedQuery("CREATE TABLE " + tableContext.asqldbTable2 + " (a CHAR MDARRAY [x,y])");
//
//                String query1 = "insert into " + tableContext.rasqlTable1 + " values decode($1)";
//                String query2 = "insert into " + tableContext.rasqlTable2 + " values decode($1)";
//                String fileName1 = benchContext.getDataDir() + tableContext.fileName1;
//                String fileName2 = benchContext.getDataDir() + tableContext.fileName2;
//
//                TimerUtil.clearTimers();
//                TimerUtil.startTimer("insert");
//                SystemController.executeShellCommand(systemController.getRasqlBinary(), "-q",
//                        query1, "-f", fileName1, "--user", "rasadmin", "--passwd", "rasadmin");
//                long time1 = TimerUtil.getElapsedMilli("insert");
//
//                TimerUtil.clearTimers();
//                TimerUtil.startTimer("insert");
//                SystemController.executeShellCommand(systemController.getRasqlBinary(), "-q",
//                        query2, "-f", fileName2, "--user", "rasadmin", "--passwd", "rasadmin");
//                long time2 = TimerUtil.getElapsedMilli("insert");
//
//
//                pr.println(report(systemController.getSystemName(), query1, tableContext.dataSize, time1));
//                pr.println(report(systemController.getSystemName(), query2, tableContext.dataSize, time2));
//
//                Integer oid1 = ((Double) RasUtil.head(RasUtil.executeRasqlQuery("select oid(c) from " + tableContext.rasqlTable1 + " as c", true, true))).intValue();
//                Integer oid2 = ((Double) RasUtil.head(RasUtil.executeRasqlQuery("select oid(c) from " + tableContext.rasqlTable2 + " as c", true, true))).intValue();
//                AsqldbConnection.executeQuery("insert into " + tableContext.asqldbTable1 + " values (" + oid1 + ");");
//                AsqldbConnection.executeQuery("insert into " + tableContext.asqldbTable2 + " values (" + oid2 + ");");
//
//                pr.flush();
//            }

        } catch (Exception ex) {
            dropCollection();
            throw ex;
        } finally {
            AsqldbConnection.commit();
        }
    }

    @Override
    public void dropCollection() {
        try {
//            for (TableContext tableContext : BenchmarkContext.dataSizes) {
//                executeTimedQuery("DROP TABLE " + tableContext.asqldbTable1);
//                executeTimedQuery("DROP TABLE " + tableContext.asqldbTable2);
//            }
            AsqldbConnection.commit();
        } catch (Exception ex) {
        }
    }
}
