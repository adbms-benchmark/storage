package system.scidb;

import benchmark.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DomainUtil;
import util.IO;
import util.ProcessExecutor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev <misev@rasdaman.com>
 */
public class SciDBSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(SciDBSystem.class);

    private static final String SYSTEM_CONTROL_KEY = "bin.system";
    private static final String CLUSTER_NAME_KEY = "cluster.name";
    
    private final String configIni;

    public SciDBSystem(String propertiesPath, BenchmarkContext benchmarkContext) throws FileNotFoundException, IOException {
        super(propertiesPath, "SciDB", benchmarkContext);

        String clusterName = getValue(CLUSTER_NAME_KEY);
        String systemControl = getValue(SYSTEM_CONTROL_KEY);

        String binDir = IO.concatPaths(installDir, "bin");
        this.configIni = IO.concatPaths(installDir, "etc/config.ini");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, systemControl), startBin, clusterName};
        this.stopCommand = new String[]{IO.concatPaths(binDir, systemControl), stopBin, clusterName};
    }

    @Override
    public void restartSystem() throws Exception {
        if (!benchmarkContext.isDisableSystemRestart()) {
            log.debug("restarting " + systemName);
            if (ProcessExecutor.executeShellCommand(stopCommand) != 0) {
                throw new Exception("Failed to stop the system.");
            }

            if (ProcessExecutor.executeShellCommand(startCommand) != 0) {
                throw new Exception("Failed to start the system.");
            }
        }
    }

    @Override
    public QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext) {
        return new SciDBAQLQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new SciDBQueryExecutor(benchmarkContext, this);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isCachingBenchmark()) {
            return new SciDBCachingBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isStorageBenchmark()) {
            return new SciDBStorageBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isOperationsBenchmark()) {
            return new SciDBOperationsBenchmarkDataManager(this, queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }

    @Override
    public void setSystemCacheSize(long bytes) throws IOException {
        Path path = Paths.get(configIni);
        Charset charset = StandardCharsets.UTF_8;

        String configIniContent = new String(Files.readAllBytes(path), charset);
        long megaBytes = bytes / DomainUtil.SIZE_1MB;
        configIniContent = configIniContent.replaceAll("mem-array-threshold=.*", "mem-array-threshold=" + megaBytes / 2);
        configIniContent = configIniContent.replaceAll("smgr-cache-size=.*", "smgr-cache-size=" + megaBytes / 2);
        Files.write(path, configIniContent.getBytes(charset));
    }
}
