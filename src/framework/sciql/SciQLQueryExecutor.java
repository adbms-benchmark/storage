package framework.sciql;

import data.DataGenerator;
import data.DomainGenerator;
import static framework.Benchmark.HOME_DIR;
import framework.context.BenchmarkContext;
import framework.context.ConnectionContext;
import framework.QueryExecutor;
import framework.context.TableContext;
import java.io.FileWriter;
import java.io.PrintWriter;
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
        TimerUtil.clearTimers();
        System.out.println("time: " + result + " ms");
        return result;
    }

    public long executeTimedQueryUpdate(String query) {
        TimerUtil.clearTimers();
        TimerUtil.startTimer("sciql query update");
        SciQLConnection.executeUpdateQuery(query);
        long result = TimerUtil.getElapsedMilli("sciql query update");
        TimerUtil.clearTimers();
        System.out.println("time: " + result + " ms");
        return result;
    }

    @Override
    public void createCollection() throws Exception {
        String benchmarkFilename = HOME_DIR + "/benchmark_insert.csv";

        try (PrintWriter pr = new PrintWriter(new FileWriter(benchmarkFilename, true))) {

            for (TableContext tableContext : BenchmarkContext.dataSizes) {

                String fileName1 = benchContext.getDataDir() + tableContext.fileName1;
                executeTimedQueryUpdate("CALL rs.attach('" + fileName1 + "')");
                String query1 = "CALL rs.import(" + tableContext.index1 + ")";
                pr.println(report(systemController.getSystemName(), query1, tableContext.dataSize,
                        executeTimedQueryUpdate(query1)));

                String fileName2 = benchContext.getDataDir() + tableContext.fileName2;
                executeTimedQueryUpdate("CALL rs.attach('" + fileName2 + "')");
                String query2 = "CALL rs.import(" + tableContext.index2 + ")";
                pr.println(report(systemController.getSystemName(), query2, tableContext.dataSize,
                        executeTimedQueryUpdate(query2)));

                pr.flush();
            }

        } catch (Exception ex) {
            dropCollection();
            throw ex;
        }
    }

    @Override
    public void dropCollection() {
        try {
            SciQLConnection.executeUpdateQuery("DELETE FROM rs.files");
            SciQLConnection.executeUpdateQuery("DELETE FROM rs.catalog");
            for (TableContext tableContext : BenchmarkContext.dataSizes) {
                try {
                    SciQLConnection.executeUpdateQuery("DROP ARRAY " + tableContext.sciqlTable1);
                } catch (Exception ex) {
                }
                try {
                    SciQLConnection.executeUpdateQuery("DROP ARRAY " + tableContext.sciqlTable2);
                } catch (Exception ex) {
                }
            }
        } catch (Exception ex) {
        }
    }
}
