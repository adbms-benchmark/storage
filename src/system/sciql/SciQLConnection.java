package system.sciql;

import system.SystemContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage SciQL connection: connect/disconnect, as well as query execution.
 *
 * @author Dimitar Misev
 */
public class SciQLConnection {

    private static final Logger log = LoggerFactory.getLogger(SciQLConnection.class);

    public static final String SCIQL_JDBC_DRIVER = "nl.cwi.monetdb.jdbc.MonetDriver";

    private static Connection connection = null;

    public static void open(SystemContext connectionContext) {
        try {
            Class.forName(SCIQL_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load the hsqldb JDBCDriver", e);
        }

        try {
            connection = DriverManager.getConnection(connectionContext.getUrl(),
                    connectionContext.getUser(), connectionContext.getPassword());
        } catch (SQLException ex) {
            throw new RuntimeException("Failed getting JDBC connection.", ex);
        }
    }

    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
            } finally {
                connection = null;
            }
        }
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeQuery(final String query) {
        log.trace("Executing query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
        } catch (SQLException e) {
            log.warn(" -> failed.", e);
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        log.trace(" -> ok.");
        return true;
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean tableExists(final String table) {
        log.trace("Check if table " + table + " exists in SciQL...");
        Statement stmt = null;
        final String query = "select count(tables.name) from tables where name = '" + table.toLowerCase() + "';";
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
            rs.next();
            int res = rs.getInt(1);
            log.trace(" -> " + (res > 0 ? "found" : "not found"));
            return res > 0;
        } catch (SQLException e) {
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        return false;
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeUpdateQuery(final String query) {
        log.trace("Executing update query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            log.warn(" -> failed.", e);
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        log.trace(" -> ok.");
        return true;
    }

    /**
     * Execute the given query.
     *
     * @return the first column of the first returned row, as an object.
     */
    public static Object executeQuerySingleResult(final String query) throws SQLException {
        List<Object> res = executeQuerySingleResult(query, 1);
        if (res.isEmpty()) {
            return null;
        } else {
            return res.get(0);
        }
    }

    /**
     * Execute the given query.
     *
     * @param columnCount number of columns per row returned by the query.
     * @return a list of the results from the first returned row, as objects.
     */
    public static List<Object> executeQuerySingleResult(final String query, int columnCount) throws SQLException {
        log.trace("Executing query: " + query);
        List<Object> ret = new ArrayList<Object>();
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    ret.add(rs.getObject(i));
                }
            }
        } catch (SQLException e) {
            log.warn(" -> failed.", e);
            throw e;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
            log.warn(" -> ok.");
        return ret;
    }

    public static void commit() {
        executeQuery("commit;");
    }

    /**
     * Execute the list of queries, return the number of passed ones.
     */
    public static int executeQueries(String[] queries) {
        int ret = 0;
        for (String query : queries) {
            if (executeQuery(query)) {
                ++ret;
            }
        }
        return ret;
    }

}
