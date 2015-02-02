package framework.context;

import java.io.IOException;
import util.IO;

public class RasdamanContext extends SystemContext {

    private static final String RASDL_BIN_KEY = "bin.rasdl";

    private final String[] startCommand;
    private final String[] stopCommand;
    private final String queryCommand;
    private final String rasdlCommand;

    public RasdamanContext(String propertiesPath) throws IOException {
        super(propertiesPath);

        String binDir = IO.concatPaths(installDir, "bin");
        String rasdlBin = getValue(RASDL_BIN_KEY);

        this.rasdlCommand = String.format("%s/%s", binDir, rasdlBin);
        this.queryCommand = String.format("%s/%s", binDir, queryBin);
        this.startCommand = new String[]{String.format("%s/%s", binDir, startBin)};
        this.stopCommand = new String[]{String.format("%s/%s", binDir, stopBin)};
    }

    public String[] getStartCommand() {
        return startCommand;
    }

    public String[] getStopCommand() {
        return stopCommand;
    }

    public String getQueryCommand() {
        return queryCommand;
    }

    public String getRasdlCommand() {
        return rasdlCommand;
    }
}
