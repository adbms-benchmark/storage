package framework;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class ConnectionContext extends Context {
    
    private final String user;
    private final String password;
    private final String url;
    private final long port;
    private final String databaseName;
    
    public static final String KEY_USER = "conn.user";
    public static final String KEY_PASSWORD = "conn.pass";
    public static final String KEY_URL = "conn.url";
    public static final String KEY_PORT = "conn.port";
    public static final String KEY_DBNAME = "conn.dbname";
    
    public ConnectionContext(String propertiesPath) throws FileNotFoundException, IOException {
        super(propertiesPath);
        this.user = getValue(KEY_USER);
        this.password = getValue(KEY_PASSWORD);
        this.url = getValue(KEY_URL);
        this.databaseName = getValue(KEY_DBNAME);
        this.port = getValueLong(KEY_PORT);
    }

    public ConnectionContext(String user, String password, String url, int port, String databaseName) {
        super();
        this.user = user;
        this.password = password;
        this.url = url;
        this.port = port;
        this.databaseName = databaseName;
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

    @Override
    public String toString() {
        return "Connection:" + "\n user=" + user + "\n password=" + password + 
                "\n url=" + url + "\n port=" + port + "\n databaseName=" + databaseName;
    }
}
