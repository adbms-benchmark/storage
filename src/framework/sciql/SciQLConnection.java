package framework.sciql;

import framework.ConnectionContext;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Manage SciQL connection: connect/disconnect, as well as query execution.
 *
 * @author Dimitar Misev
 */
public class SciQLConnection {

    public static final String SCIQL_JDBC_DRIVER = "nl.cwi.monetdb.jdbc.MonetDriver";

    private static Connection connection = null;

    public static void open(ConnectionContext connectionContext) {
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
        System.out.print("  executing query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
        return true;
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeUpdateQuery(final String query) {
        System.out.print("  executing query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
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
        System.out.print("  executing query: " + query);
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
            System.out.println(" ... failed.");
            throw e;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
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
