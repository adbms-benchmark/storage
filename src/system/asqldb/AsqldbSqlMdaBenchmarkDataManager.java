/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system.asqldb;

import data.RandomDataGenerator;
import data.DomainGenerator;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import benchmark.sqlmda.BenchmarkContextGenerator;
import benchmark.sqlmda.BenchmarkContextJoin;
import system.rasdaman.RasdamanQueryExecutor;
import system.rasdaman.RasdamanQueryGenerator;
import system.rasdaman.RasdamanTypeManager;
import java.io.File;
import java.util.List;
import org.asqldb.ras.RasUtil;
import org.asqldb.util.AsqldbConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.BenchmarkUtil;
import util.IO;
import util.Pair;
import util.ProcessExecutor;
import util.StopWatch;

/**
 *
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class AsqldbSqlMdaBenchmarkDataManager extends DataManager<AsqldbSystem> {
    
    private static final Logger log = LoggerFactory.getLogger(AsqldbSqlMdaBenchmarkDataManager.class);
    
    private final RasdamanTypeManager typeManager;

    public AsqldbSqlMdaBenchmarkDataManager(AsqldbSystem systemController, 
            QueryExecutor<AsqldbSystem> queryExecutor, BenchmarkContext benchmarkContext) {
        super(systemController, queryExecutor, benchmarkContext);
        typeManager = new RasdamanTypeManager(new RasdamanQueryExecutor(systemController, benchmarkContext));
    }
    
    @Override
    public long loadData() throws Exception {

        QueryGenerator queryGenerator = systemController.getQueryGenerator(benchmarkContext);
        List<Pair<String, BenchmarkContext>> createQueries = queryGenerator.getCreateQueries();
        long totalTime = 0;

        for (Pair<String, BenchmarkContext> createQuery : createQueries) {
            BenchmarkContext bc = createQuery.getSecond();
            String asqldbTableName = bc.getArrayName();
            String rasCollName = BenchmarkUtil.getAsqldbCollectionNameInRasdaman(asqldbTableName, "v");
            if (RasUtil.collectionExists(rasCollName)) {
                log.info("Array " + asqldbTableName + " found, not reingesting.");
                return totalTime;
            }
            AsqldbConnection.executeUpdateQuery(createQuery.getFirst());

            String insertQuery = "insert into " + rasCollName + " values $1";
            Pair<String, String> rasType = typeManager.createType(bc.getArrayDimensionality(), "char");
            List<Pair<Long, Long>> domainBoundaries = new DomainGenerator(
                    bc.getArrayDimensionality()).getDomainBoundaries(bc.getArraySize());
            dataGenerator = new RandomDataGenerator(bc.getArraySize(), bc.getDataDir());
            String filePath = dataGenerator.getFilePath();

            StopWatch timer = new StopWatch();
            ProcessExecutor.executeShellCommand(
                    systemController.getQueryCommand(),
                    "-q", insertQuery,
                    "--mddtype", rasType.getFirst(),
                    "--mdddomain", RasdamanQueryGenerator.convertToRasdamanDomain(domainBoundaries),
                    "-f", filePath);
            Integer oid = ((Double) RasUtil.head(RasUtil.executeRasqlQuery(
                    "select oid(c) from " + rasCollName + " as c", true, true))).intValue();
            AsqldbConnection.executeQuery("insert into " + asqldbTableName + " values (" + oid + ");");
            AsqldbConnection.commit();
            long insertTime = timer.getElapsedTime();
            totalTime += insertTime;

            File resultsDir = IO.getResultsDir();
            File insertResultFile = new File(resultsDir.getAbsolutePath(), "asqldb_insert_results.csv");
            IO.appendLineToFile(insertResultFile.getAbsolutePath(),
                    String.format("\"%s\", \"%d\", \"%d\", \"%d\", \"%d\"",
                            asqldbTableName, bc.getArraySize(), -1, bc.getArrayDimensionality(), insertTime));
        }
        return totalTime;
    }

    @Override
    public long dropData() {
        StopWatch timer = new StopWatch();
        try {
            if (benchmarkContext.isSqlMdaBenchmark()) {
                List<BenchmarkContext> benchContexts = BenchmarkContextGenerator.generate(benchmarkContext);
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
        return timer.getElapsedTime();
    }
}
