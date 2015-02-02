package framework;

import framework.context.ConnectionContext;
import framework.rasdaman.RasdamanSystemController;
import framework.scidb.SciDBSystemController;
import framework.sciql.SciQLSystemController;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author George Merticariu
 */
public abstract class SystemController extends ConnectionContext {

    protected String[] startSystemCommand;
    protected String[] stopSystemCommand;
    protected String systemName;

    public SystemController(String propertiesPath, String systemName) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.systemName = systemName;
    }

    protected SystemController(String propertiesPath, String[] startSystemCommand, String[] stopSystemCommand, String systemName) throws IOException {
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

    @Override
    public String toString() {
        return systemName + "System Controller:" + "\n startSystemCommand=" + arrayToString(startSystemCommand) +
                "\n stopSystemCommand=" + arrayToString(stopSystemCommand);
    }

    public static SystemController getSystemController(String system, String configFile) throws IOException {
        switch (system) {
                case "rasdaman":
                    return new RasdamanSystemController(configFile);
                case "sciql":
                    return new SciDBSystemController(configFile);
                case "scidb":
                    return new SciQLSystemController(configFile);
                default:
                    throw new IllegalArgumentException("System " + system + " not supported.");
        }
    }
}
