package framework;

import framework.context.ConnectionContext;
import framework.context.Context;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 * @author George Merticariu
 */
public abstract class SystemController extends Context {

    protected String[] startSystemCommand;
    protected String[] stopSystemCommand;
    protected String systemName;
    protected ConnectionContext connContext;

    public SystemController(String propertiesPath, ConnectionContext connContext) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.connContext = connContext;
    }

    protected SystemController(String[] startSystemCommand, String[] stopSystemCommand, String systemName) {
        this.startSystemCommand = startSystemCommand;
        this.stopSystemCommand = stopSystemCommand;
        this.systemName = systemName;
        this.connContext = null;
    }

    public abstract void restartSystem() throws Exception;

    public static int executeShellCommand(String... command) {
        return executeShellCommandRedirect("/dev/null", command);
    }

    public static int executeShellCommandRedirect(String output, String... command) {
        ProcessExecutor processExecutor = new ProcessExecutor(command);
        try {
            processExecutor.executeRedirect(output);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
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
}
