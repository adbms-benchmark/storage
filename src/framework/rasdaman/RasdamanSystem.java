package framework.rasdaman;

import framework.AdbmsSystem;
import framework.DataManager;
import framework.QueryExecutor;
import framework.QueryGenerator;
import framework.context.BenchmarkContext;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.Pair;
import util.ProcessExecutor;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class RasdamanSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(RasdamanSystem.class);

    public RasdamanSystem(String propertiesPath) throws IOException {
        super(propertiesPath, RASDAMAN_SYSTEM_NAME);
        String binDir = IO.concatPaths(installDir, "bin");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, startBin)};
        this.stopCommand = new String[]{IO.concatPaths(binDir, stopBin)};
    }

    @Override
    public void restartSystem() throws Exception {
        log.debug("restarting " + systemName);
        if (ProcessExecutor.executeShellCommand(stopCommand) != 0) {
            throw new Exception("Failed to stop the system.");
        }

        if (ProcessExecutor.executeShellCommand(startCommand) != 0) {
            throw new Exception("Failed to start the system.");
        }
    }
    
    private String getDimNames(int noOfDimensions) {
        StringBuilder dimNames = new StringBuilder("");
        for (int i = 0; i < noOfDimensions; i++) {
            if (i > 0) {
                dimNames.append(",");
            }
            dimNames.append("d" + i);
        }
        return dimNames.toString();
    }
    
    private String getBands(String... baseTypes) {
        StringBuilder bands = new StringBuilder("");
        for (int i = 0; i < baseTypes.length; i++) {
            if (i > 0) {
                bands.append(",");
            }
            bands.append("att").append(i).append(" ").append(baseTypes[i]);
        }
        return bands.toString();
    }
    
    public Pair<String, String> createRasdamanType(int noOfDimensions, String... baseTypes) throws Exception {
        String baseType = baseTypes[0];
        if (baseTypes.length > 0) {
            baseType = MessageFormat.format("{0}{1}", baseType, baseTypes.length);
            String baseTypeDefinition = MessageFormat.format("create type {0} as ({1})", baseType, getBands(baseTypes));
            ProcessExecutor.executeShellCommand(queryCommand,
                    "-q", baseTypeDefinition,
                    "--user", getUser(),
                    "--passwd", getPassword());
        }
        
        String mddTypeName = MessageFormat.format("B_MDD_{0}_{1}", baseType, noOfDimensions);
        String setTypeName = MessageFormat.format("B_SET_{0}_{1}", baseType, noOfDimensions);
        String mddTypeDefinition = MessageFormat.format("create type {0} as {1} mdarray [ {2} ]", mddTypeName, baseType, getDimNames(noOfDimensions));
        String setTypeDefinition = MessageFormat.format("create type {0} as set ({1})", setTypeName, mddTypeName);
        ProcessExecutor.executeShellCommand(queryCommand,
                "-q", mddTypeDefinition,
                "--user", getUser(),
                "--passwd", getPassword());
        ProcessExecutor.executeShellCommand(queryCommand,
                "-q", setTypeDefinition,
                "--user", getUser(),
                "--passwd", getPassword());
        return Pair.of(mddTypeName, setTypeName);
    }

    public void deleteRasdamanType(String mddTypeName, String setTypeName) {
        if (ProcessExecutor.executeShellCommand(queryCommand,
                "-q", "drop type " + setTypeName,
                "--user", getUser(),
                "--passwd", getPassword()) != 0) {
            System.out.printf("Faild to delete set type");
        }
        if (ProcessExecutor.executeShellCommand(queryCommand,
                "-q", "drop type " + mddTypeName,
                "--user", getUser(),
                "--passwd", getPassword()) != 0) {
            System.out.printf("Faild to delete set type");
        }
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new RasdamanQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new RasdamanQueryExecutor(this, benchmarkContext);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isCachingBenchmark()) {
            return new RasdamanCachingBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isStorageBenchmark()) {
            return new RasdamanStorageBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }
}
