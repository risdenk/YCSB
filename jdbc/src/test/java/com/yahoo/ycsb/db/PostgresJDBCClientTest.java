package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

/**
 * Created by risdenk on 11/4/15.
 */
public class PostgresJDBCClientTest extends JdbcDBClientTest {
    private static final String TEST_DB_URL = "jdbc:postgres://127.0.0.1:5432/ycsb";
    private static final String TEST_DB_USER = "postgres";
    private static final String TEST_DB_PASSWORD = "";
    private static final int POSTGRES_DEFAULT_PORT = 5432;

    @BeforeClass
    public static void setup() {
        // Test if we can connect.
        Socket socket = null;
        try {
            // Connect
            socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), POSTGRES_DEFAULT_PORT), 100);
            assertThat("Socket is not bound.", socket.getLocalPort(), not(-1));
        } catch (IOException connectFailed) {
            assumeNoException("MySQL is not running. Skipping tests.", connectFailed);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignore) {
                    // Ignore.
                }
            }
            socket = null;
        }

        try {
            jdbcConnection = DriverManager.getConnection(TEST_DB_URL, TEST_DB_USER, TEST_DB_PASSWORD);

            jdbcDBClient = new JdbcDBClient();

            Properties p = new Properties();
            p.setProperty(JdbcDBClientConstants.CONNECTION_URL, TEST_DB_URL);
            p.setProperty(JdbcDBClientConstants.CONNECTION_USER, TEST_DB_USER);
            p.setProperty(JdbcDBClientConstants.CONNECTION_PASSWD, TEST_DB_PASSWORD);

            jdbcDBClient.setProperties(p);
            jdbcDBClient.init();
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Could not create local Database");
        } catch (DBException e) {
            e.printStackTrace();
            fail("Could not create JdbcDBClient instance");
        }
    }

    @AfterClass
    public static void teardown() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
            if (jdbcDBClient != null) {
                jdbcDBClient.cleanup();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Could not close local Database connection");
        } catch (DBException e) {
            e.printStackTrace();
            fail("Could not cleanup client connection");
        }
    }

    @Before
    public void prepareTest() {
        try {
            DatabaseMetaData metaData = jdbcConnection.getMetaData();
            ResultSet tableResults = metaData.getTables(null, null, TABLE_NAME, null);
            if (tableResults.next()) {
                // If the table already exists, just truncate it
                jdbcConnection.prepareStatement(
                        String.format("TRUNCATE TABLE %s", TABLE_NAME)
                ).execute();
            } else {
                // If the table does not exist then create it
                StringBuilder createString = new StringBuilder(
                        String.format("CREATE TABLE %s (%s VARCHAR(100) PRIMARY KEY", TABLE_NAME, KEY_FIELD)
                );
                for (int i = 0; i < NUM_FIELDS; i++) {
                    createString.append(
                            String.format(", %s%d VARCHAR(100)", FIELD_PREFIX, i)
                    );
                }
                createString.append(")");
                jdbcConnection.prepareStatement(createString.toString()).execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Failed to prepare test");
        }
    }
}
