package framework;

import framework.asqldb.AsqldbSystem;
import framework.context.BenchmarkContext;
import framework.context.SystemContext;
import framework.rasdaman.RasdamanSystem;
import framework.scidb.SciDBSystem;
import framework.sciql.SciQLSystem;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StopWatch;
import util.StringUtil;

/**
 * Wrapps Array DBMS system-specific functionality, like restarting the system.
 *
 * @author Dimitar Misev
 * @author George Merticariu
 */
public abstract class AdbmsSystem extends SystemContext {

    private static final Logger log = LoggerFactory.getLogger(AdbmsSystem.class);

    public static final String RASDAMAN_SYSTEM_NAME = "rasdaman";
    public static final String SCIDB_SYSTEM_NAME = "SciDB";
    public static final String SCIQL_SYSTEM_NAME = "SciQL";
    public static final String ASQLDB_SYSTEM_NAME = "ASQLDB";

    protected String systemName;

    public AdbmsSystem(String propertiesPath, String systemName) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.systemName = systemName;
        this.startCommand = new String[]{startBin};
        this.stopCommand = new String[]{stopBin};
    }

    protected AdbmsSystem(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand, String systemName) throws IOException {
        super(propertiesPath);
        this.startCommand = startSystemCommand;
        this.stopCommand = stopSystemCommand;
        this.systemName = systemName;
    }

    public abstract void restartSystem() throws Exception;

    public abstract QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext);

    public abstract QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException;

    @Override
    public String toString() {
        return systemName + " System:"
                + "\n startSystemCommand=" + StringUtil.arrayToString(startCommand)
                + "\n stopSystemCommand=" + StringUtil.arrayToString(stopCommand);
    }

    public static AdbmsSystem getSystemController(String system, String configFile) throws IOException {
        switch (system) {
            case "rasdaman":
                return new RasdamanSystem(configFile);
            case "sciql":
                return new SciQLSystem(configFile);
            case "scidb":
            return new SciDBSystem(configFile);
            case "asqldb":
                return new AsqldbSystem(configFile);
            default:
                throw new IllegalArgumentException("System " + system + " not supported.");
        }
    }

    public static int executeShellCommand(String... command) {
        return executeShellCommandRedirect(ProcessExecutor.DEV_NULL, command);
    }

    public static int executeShellCommandRedirect(String output, String... command) {
        String cmd = StringUtil.arrayToString(command) + " > " + output;
        log.debug("executing shell command: " + cmd);

        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            StopWatch timer = new StopWatch();
            processExecutor.executeRedirectOutput(output);
            log.trace(" -> finished in " + timer.getElapsedTime() + " ms.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        if (processExecutor.getExitStatus() != 0) {
            log.error("shell command failed: " + cmd);
            log.error(" -> error: " + processExecutor.getError());
        }

        return processExecutor.getExitStatus();
    }

    public static String executeShellCommandOutput(String... command) {
        String cmd = StringUtil.arrayToString(command);
        log.debug("executing shell command: " + cmd);

        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            StopWatch timer = new StopWatch();
            processExecutor.execute();
            log.trace(" -> finished in " + timer.getElapsedTime() + " ms.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        if (processExecutor.getExitStatus() != 0) {
            log.error("shell command failed: " + cmd);
            log.error(" -> error: " + processExecutor.getError());
        }

        return processExecutor.getOutput();
    }

    public String getSystemName() {
        return systemName;
    }
}
