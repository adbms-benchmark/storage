package framework.context;

import java.io.IOException;
import util.IO;

public class RasdamanContext extends SystemContext {

    private static final String RASDL_BIN_KEY = "bin.rasdl";

    private final String rasdlCommand;

    public RasdamanContext(String propertiesPath) throws IOException {
        super(propertiesPath);

        String binDir = IO.concatPaths(installDir, "bin");
        String rasdlBin = getValue(RASDL_BIN_KEY);

        this.rasdlCommand = IO.concatPaths(binDir, rasdlBin);
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, startBin)};
        this.stopCommand = new String[]{IO.concatPaths(binDir, stopBin)};
    }

    public String getRasdlCommand() {
        return rasdlCommand;
    }
}
