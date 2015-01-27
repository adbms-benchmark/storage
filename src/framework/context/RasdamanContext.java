package framework.context;

import java.io.IOException;

public class RasdamanContext extends ConnectionContext {

    private static final String BIN_DIR_KEY = "bin.dir";
    private static final String EXECUTE_QUERY_BIN_KEY = "bin.query";

    private static final String RASDL_BIN_KEY = "bin.rasdl";
    private static final String START_COMMAND_KEY = "command.start";
    private static final String STOP_COMMAND_KEY = "command.stop";

    private final String[] startCommand;
    private final String[] stopCommand;
    private final String executeQueryBin;
    private final String rasdlBin;

    public RasdamanContext(String propertiesPath) throws IOException {
        super(propertiesPath);

        String binDir = getValue(BIN_DIR_KEY);
        String executeQueryPath = getValue(EXECUTE_QUERY_BIN_KEY);
        String startCommand = getValue(START_COMMAND_KEY);
        String stopCommand = getValue(STOP_COMMAND_KEY);
        String rasdlBin = getValue(RASDL_BIN_KEY);

        this.rasdlBin = String.format("%s/%s", binDir, rasdlBin);

        this.executeQueryBin = String.format("%s/%s", binDir, executeQueryPath);
        this.startCommand = new String[]{String.format("%s/%s", binDir, startCommand)};
        this.stopCommand = new String[]{String.format("%s/%s", binDir, stopCommand)};
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

    public String getRasdlBin() {
        return rasdlBin;
    }
}
