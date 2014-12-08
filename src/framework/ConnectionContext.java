package framework;

/**
 *
 * @author George Merticariu
 * @author Dimitar Misev
 */
public class ConnectionContext {

    private final String user;
    private final String password;
    private final String url;
    private final int port;
    private final String databaseName;

    public ConnectionContext(String user, String password, String url, int port, String databaseName) {
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

    public int getPort() {
        return port;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
