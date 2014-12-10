package framework;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 *
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

        int exitCode = -1;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command).redirectOutput(new File(output));
            Process p = processBuilder.start();
            exitCode = p.waitFor();
            if (exitCode != 0) {
                //TODO-GM: change error reporting to log file
                try (Scanner scan = new Scanner(p.getErrorStream())) {
                    while (scan.hasNextLine()) {
                        System.out.println(scan.nextLine());
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            //TODO-GM: log to file
            e.printStackTrace();
        }

        return exitCode;
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
