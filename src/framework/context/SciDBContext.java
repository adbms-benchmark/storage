package framework.context;

import java.io.IOException;
import util.IO;

public class SciDBContext extends SystemContext {

    private static final String SYSTEM_CONTROL_KEY = "bin.system";
    private static final String CLUSTER_NAME_KEY = "cluster.name";

    public SciDBContext(String propertiesPath) throws IOException {
        super(propertiesPath);

        String clusterName = getValue(CLUSTER_NAME_KEY);
        String systemControl = getValue(SYSTEM_CONTROL_KEY);

        String binDir = IO.concatPaths(installDir, "bin");
        this.queryCommand = IO.concatPaths(binDir, queryBin);
        this.startCommand = new String[]{IO.concatPaths(binDir, systemControl), IO.concatPaths(binDir, startBin), clusterName};
        this.stopCommand = new String[]{IO.concatPaths(binDir, systemControl), IO.concatPaths(binDir, stopBin), clusterName};
    }
}
