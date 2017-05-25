package benchmark;

import system.asqldb.AsqldbSystem;
import system.SystemContext;
import system.rasdaman.RasdamanSystem;
import system.scidb.SciDBSystem;
import system.sciql.SciQLSystem;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StringUtil;

/**
 * Wrapps Array DBMS system-specific functionality, like restarting the system,
 * executing queries and creating/dropping data.
 *
 * @author Dimitar Misev
 * @author George Merticariu
 */
public abstract class AdbmsSystem extends SystemContext {

    private static final Logger log = LoggerFactory.getLogger(AdbmsSystem.class);

    public static final String RASDAMAN_SYSTEM_NAME = "rasdaman";
    public static final String SCIDB_SYSTEM_NAME = "scidb";
    public static final String SCIQL_SYSTEM_NAME = "sciql";
    public static final String ASQLDB_SYSTEM_NAME = "asqldb";

    protected String systemName;
    protected final BenchmarkContext benchmarkContext;

    public AdbmsSystem(String propertiesPath, String systemName, BenchmarkContext benchmarkContext) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.systemName = systemName;
        this.startCommand = new String[]{startBin};
        this.stopCommand = new String[]{stopBin};
        this.benchmarkContext = benchmarkContext;
    }

    protected AdbmsSystem(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand, String systemName, BenchmarkContext benchmarkContext) throws IOException {
        super(propertiesPath);
        this.startCommand = startSystemCommand;
        this.stopCommand = stopSystemCommand;
        this.systemName = systemName;
        this.benchmarkContext = benchmarkContext;
    }

    public abstract void restartSystem() throws Exception;
    
    public abstract void setSystemCacheSize(long bytes) throws IOException;

    public abstract QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext);

    public abstract QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException;
    
    public abstract DataManager getDataManager(BenchmarkContext benchmarkContext, QueryExecutor queryExecutor);

    /**
     * Factory method for getting a concrete ADBMS system controller.
     * @param system system identifier
     * @param configFile system configuration file
     * @return the concrete ADBMS controller
     * @throws IOException if reading the config file fails.
     */
    public static AdbmsSystem getAdbmsSystem(String system, String configFile, BenchmarkContext benchmarkContext) throws IOException {
        switch (system) {
            case RASDAMAN_SYSTEM_NAME:
                return new RasdamanSystem(configFile, benchmarkContext);
            case SCIQL_SYSTEM_NAME:
                return new SciQLSystem(configFile, benchmarkContext);
            case SCIDB_SYSTEM_NAME:
                return new SciDBSystem(configFile, benchmarkContext);
            case ASQLDB_SYSTEM_NAME:
                return new AsqldbSystem(configFile, benchmarkContext);
            default:
                throw new IllegalArgumentException("System " + system + " not supported.");
        }
    }

    public String getSystemName() {
        return systemName;
    }

    @Override
    public String toString() {
        return systemName + " System:"
                + "\n startSystemCommand=" + StringUtil.arrayToString(startCommand)
                + "\n stopSystemCommand=" + StringUtil.arrayToString(stopCommand);
    }
}
