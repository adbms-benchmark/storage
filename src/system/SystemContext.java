package system;

import benchmark.Context;
import java.io.IOException;

/**
 * System configuration context.
 * 
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class SystemContext extends Context {

    protected final String user;
    protected final String password;
    protected final String url;
    protected final long port;
    protected final String databaseName;
    protected final String installDir;
    protected final String dataDir;
    protected final String queryBin;
    protected final String startBin;
    protected final String stopBin;

    public static final String KEY_USER = "conn.user";
    public static final String KEY_PASSWORD = "conn.pass";
    public static final String KEY_URL = "conn.url";
    public static final String KEY_PORT = "conn.port";
    public static final String KEY_DBNAME = "conn.dbname";

    public static final String KEY_INSTALL_DIR = "install.dir";
    public static final String KEY_DATA_DIR = "data.dir";
    public static final String KEY_QUERY_BIN = "bin.query";
    public static final String KEY_START_BIN = "command.start";
    public static final String KEY_STOP_BIN = "command.stop";

    // commands with absolute paths
    protected String[] startCommand;
    protected String[] stopCommand;
    protected String queryCommand;

    public SystemContext(String propertiesPath) throws IOException {
        super(propertiesPath);
        this.user = getValue(KEY_USER);
        this.password = getValue(KEY_PASSWORD);
        this.url = getValue(KEY_URL);
        this.databaseName = getValue(KEY_DBNAME);
        this.port = getValueLong(KEY_PORT);
        this.installDir = getValue(KEY_INSTALL_DIR);
        this.dataDir = getValue(KEY_DATA_DIR);
        this.queryBin = getValue(KEY_QUERY_BIN);
        this.startBin = getValue(KEY_START_BIN);
        this.stopBin = getValue(KEY_STOP_BIN);
    }

    public SystemContext(String user, String password, String url, int port, String databaseName,
            String installDir, String dataDir, String queryBin, String startCommand, String stopCommand) {
        super();
        this.user = user;
        this.password = password;
        this.url = url;
        this.port = port;
        this.databaseName = databaseName;
        this.installDir = installDir;
        this.dataDir = dataDir;
        this.queryBin = queryBin;
        this.startBin = startCommand;
        this.stopBin = stopCommand;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public long getPort() {
        return port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getInstallDir() {
        return installDir;
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getQueryBin() {
        return queryBin;
    }

    public String getStartBin() {
        return startBin;
    }

    public String getStopBin() {
        return stopBin;
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

    @Override
    public String toString() {
        return "Connection:" + "\n user=" + user + "\n password=" + password
                + "\n url=" + url + "\n port=" + port + "\n databaseName=" + databaseName
                + "\n installDir=" + installDir + "\n dataDir=" + dataDir + "\n queryBin=" + queryBin;
    }
}
