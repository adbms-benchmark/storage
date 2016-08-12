package system.rasdaman;

import benchmark.AdbmsSystem;
import benchmark.DataManager;
import benchmark.QueryExecutor;
import benchmark.QueryGenerator;
import benchmark.BenchmarkContext;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IO;
import util.ProcessExecutor;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class RasdamanSystem extends AdbmsSystem {

    private static final Logger log = LoggerFactory.getLogger(RasdamanSystem.class);
    
    private final String rasmgrConf;

    public RasdamanSystem(String propertiesPath, BenchmarkContext benchmarkContext) throws IOException {
        super(propertiesPath, RASDAMAN_SYSTEM_NAME, benchmarkContext);
        String binDir = IO.concatPaths(installDir, "bin");
        this.rasmgrConf = IO.concatPaths(installDir, "etc/rasmgr.conf");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, startBin)};
        this.stopCommand = new String[]{IO.concatPaths(binDir, stopBin)};
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
        return new RasdamanQueryGenerator(benchmarkContext);
    }

    @Override
    public QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException {
        return new RasdamanQueryExecutor(this, benchmarkContext);
    }

    @Override
    public DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor) {
        if (benchmarkContext.isCachingBenchmark()) {
            return new RasdamanCachingBenchmarkDataManager(this, (RasdamanQueryExecutor) queryExecutor, benchmarkContext);
        } else if (benchmarkContext.isStorageBenchmark()) {
            return new RasdamanStorageBenchmarkDataManager(this, (RasdamanQueryExecutor) queryExecutor, benchmarkContext);
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type '" + benchmarkContext.getBenchmarkType() + "'.");
        }
    }

    @Override
    public void setSystemCacheSize(long bytes) throws IOException {
        Path path = Paths.get(rasmgrConf);
        Charset charset = StandardCharsets.UTF_8;

        String rasmgrConfContent = new String(Files.readAllBytes(path), charset);
        rasmgrConfContent = rasmgrConfContent.replaceAll("define cache -size .*", "define cache -size " + bytes);
        Files.write(path, rasmgrConfContent.getBytes(charset));
    }
}
