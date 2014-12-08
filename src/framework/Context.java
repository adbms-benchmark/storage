package framework;

/**
 *
 * @author George Merticariu
 */
public class Context {

    private String user;
    private String password;
    private String url;
    private int port;
    private String databaseName;

    public Context(String user, String password, String url, int port, String databaseName) {
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
