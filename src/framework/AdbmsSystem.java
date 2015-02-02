package framework;

import framework.asqldb.AsqldbSystem;
import framework.context.BenchmarkContext;
import framework.context.SystemContext;
import framework.rasdaman.RasdamanSystem;
import framework.scidb.SciDBSystem;
import framework.sciql.SciQLSystem;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Wrapps Array DBMS system-specific functionality, like restarting the system.
 * 
 * @author Dimitar Misev
 * @author George Merticariu
 */
public abstract class AdbmsSystem extends SystemContext {
    
    public static final String RASDAMAN_SYSTEM_NAME = "rasdaman";
    public static final String SCIDB_SYSTEM_NAME = "SciDB";
    public static final String SCIQL_SYSTEM_NAME = "SciQL";
    public static final String ASQLDB_SYSTEM_NAME = "ASQLDB";

    protected String[] startSystemCommand;
    protected String[] stopSystemCommand;
    protected String systemName;

    public AdbmsSystem(String propertiesPath, String systemName) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.systemName = systemName;
    }

    protected AdbmsSystem(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand, String systemName) throws IOException {
        super(propertiesPath);
        this.startSystemCommand = startSystemCommand;
        this.stopSystemCommand = stopSystemCommand;
        this.systemName = systemName;
    }

    public abstract void restartSystem() throws Exception;

    public static int executeShellCommand(String... command) {
        return executeShellCommandRedirect("/dev/null", command);
    }

    public static int executeShellCommandRedirect(String output, String... command) {
        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            processExecutor.executeRedirectOutput(output);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        if (processExecutor.getExitStatus() != 0){
            System.err.println("----------------------------------------");
            System.err.println("Command failed");
            System.err.print("Command:");
            for (int i = 0; i < command.length; i++) {
                System.err.print(" ");
                System.err.print(command[i]);
            }
            System.err.println("");
            System.err.println("");
            System.err.println(processExecutor.getError());
            System.err.println("----------------------------------------------------------");
        }

        return processExecutor.getExitStatus();
    }

    public String getSystemName() {
        return systemName;
    }

    private String arrayToString(String[] array) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(array[i]);
        }
        return sb.toString();
    }
    
    public abstract QueryGenerator getQueryGenerator(BenchmarkContext benchmarkContext);
    
    public abstract QueryExecutor getQueryExecutor(BenchmarkContext benchmarkContext) throws IOException;

    @Override
    public String toString() {
        return systemName + "System Controller:" + "\n startSystemCommand=" + arrayToString(startSystemCommand) +
                "\n stopSystemCommand=" + arrayToString(stopSystemCommand);
    }

    public static AdbmsSystem getSystemController(String system, String configFile) throws IOException {
        switch (system) {
                case "rasdaman":
                    return new RasdamanSystem(configFile);
                case "sciql":
                    return new SciDBSystem(configFile);
                case "scidb":
                    return new SciQLSystem(configFile);
                case "asqldb":
                    return new AsqldbSystem(configFile);
                default:
                    throw new IllegalArgumentException("System " + system + " not supported.");
        }
    }
}
