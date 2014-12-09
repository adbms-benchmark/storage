package framework;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class ConnectionContext {
    
    private final Properties properties;
    private final String user;
    private final String password;
    private final String url;
    private final int port;
    private final String databaseName;
    
    public static final String KEY_USER = "conn.user";
    public static final String KEY_PASSWORD = "conn.password";
    public static final String KEY_URL = "conn.url";
    public static final String KEY_PORT = "conn.port";
    public static final String KEY_DBNAME = "conn.dbname";
    
    public ConnectionContext(String propertiesPath) throws FileNotFoundException, IOException {
        properties = new Properties();
        properties.load(new FileInputStream(propertiesPath));
        this.user = properties.getProperty(KEY_USER);
        this.password = properties.getProperty(KEY_PASSWORD);
        this.url = properties.getProperty(KEY_URL);
        this.databaseName = properties.getProperty(KEY_DBNAME);
        int p = 35000;
        try {
            p = Integer.parseInt(properties.getProperty(KEY_PORT));
        } catch (Exception ex) {
        } finally {
            port = p;
        }
    }

    public ConnectionContext(String user, String password, String url, int port, String databaseName) {
        this.user = user;
        this.password = password;
        this.url = url;
        this.port = port;
        this.databaseName = databaseName;
        this.properties = null;
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

    public int getPort() {
        return port;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
