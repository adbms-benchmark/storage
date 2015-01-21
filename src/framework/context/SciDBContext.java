package framework.context;

import java.io.IOException;

public class SciDBContext extends ConnectionContext {
    private static final String BIN_DIR_KEY = "bin.dir";
    private static final String EXECUTE_QUERY_BIN_KEY = "bin.query";
    private static final String SYSTEM_CONTROL_KEY = "bin.system";
    private static final String CLUSTER_NAME_KEY = "cluster.name";
    private static final String START_COMMAND_KEY = "command.start";
    private static final String STOP_COMMAND_KEY = "command.stop";

    private final String[] startCommand;
    private final String[] stopCommand;
    private final String executeQueryBin;

    public SciDBContext(String propertiesPath) throws IOException {
        super(propertiesPath);

        String binDir = getValue(BIN_DIR_KEY);
        String executeQueryPath = getValue(EXECUTE_QUERY_BIN_KEY);
        String clusterName = getValue(CLUSTER_NAME_KEY);
        String systemControl = getValue(SYSTEM_CONTROL_KEY);
        String startCommand = getValue(START_COMMAND_KEY);
        String stopCommand = getValue(STOP_COMMAND_KEY);

        this.executeQueryBin = String.format("%s/%s", binDir, executeQueryPath);
        this.startCommand = new String[]{String.format("%s/%s", binDir, systemControl), startCommand, clusterName};
        this.stopCommand = new String[]{String.format("%s/%s", binDir, systemControl), stopCommand, clusterName};
    }

    public String[] getStartCommand() {
        return startCommand;
    }

    public String[] getStopCommand() {
        return stopCommand;
    }

    public String getExecuteQueryBin() {
        return executeQueryBin;
    }
}
