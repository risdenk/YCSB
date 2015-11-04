package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.fail;

/**
 * Created by risdenk on 11/4/15.
 */
public class MySQLJDBCClientTest extends JdbcDBClientTest {
    private static final String TEST_DB_URL = "jdbc:mysql://127.0.0.1:3306/";
    private static final String TEST_DB_USER = "root";
    private static final String TEST_DB_PASSWORD = "";

    @BeforeClass
    public static void setup() {
        try {
            jdbcConnection = DriverManager.getConnection(TEST_DB_URL, TEST_DB_USER, TEST_DB_PASSWORD);
            try (Statement stmt = jdbcConnection.createStatement()) {
                String sql = "CREATE DATABASE IF NOT EXISTS " + TEST_DB_NAME;
                stmt.executeUpdate(sql);
            }
            jdbcConnection.setCatalog(TEST_DB_NAME);

            jdbcDBClient = new JdbcDBClient();

            Properties p = new Properties();
            p.setProperty(JdbcDBClientConstants.CONNECTION_URL, TEST_DB_URL + TEST_DB_NAME);
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
            try (Statement stmt = jdbcConnection.createStatement()) {
                String sql = "DROP DATABASE IF EXISTS " + TEST_DB_NAME;
                stmt.executeUpdate(sql);
            }

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
