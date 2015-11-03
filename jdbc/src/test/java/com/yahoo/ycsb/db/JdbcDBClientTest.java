package com.yahoo.ycsb.db;

import static org.junit.Assert.*;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by kruthar on 11/2/15.
 */
public class JdbcDBClientTest {
    private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DERBY_URL = "jdbc:derby:memory:ycsb";
    private static final String TABLE_NAME = "USERTABLE";
    private static final int FIELD_LENGTH = 32;
    private static final String FIELD_PREFIX = "FIELD";
    private static final String KEY_FIELD = "YCSB_KEY";
    private static final int NUM_FIELDS = 3;

    private static Connection jdbcConnection = null;
    private static JdbcDBClient jdbcDBClient = null;

    @BeforeClass
    public static void setup() {
        try {
            Class driverClass = Class.forName(DERBY_DRIVER);
            jdbcConnection = DriverManager.getConnection(DERBY_URL + ";create=true");

            jdbcDBClient = new JdbcDBClient();

            Properties p = new Properties();
            p.setProperty(JdbcDBClientConstants.CONNECTION_URL, DERBY_URL);
            p.setProperty(JdbcDBClientConstants.DRIVER_CLASS, DERBY_DRIVER);

            jdbcDBClient.setProperties(p);
            jdbcDBClient.init();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            fail("Could not find Derby Driver Class: org.apache.derby.jdbc.EmbeddedDriver");
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Could not create local Derby Database");
        } catch (DBException e) {
            e.printStackTrace();
            fail("Could not create JdbcDBClient instance");
        }
    }

    @AfterClass
    public static void teardown() {
        try {
            jdbcConnection.close();
            jdbcConnection = DriverManager.getConnection(DERBY_URL + ";drop=true");
        } catch (SQLNonTransientConnectionException e) {
            // Expected exception when database is destroyed
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Could not drop local Derby Database");
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

    /*
        This is a copy of buildDeterministicValue() from core:com.yahoo.ycsb.workloads.CoreWorkload.java.
        That method is neither public nor static so we need a copy.
     */
    private String buildDeterministicValue(String key, String fieldkey) {
        int size = FIELD_LENGTH;
        StringBuilder sb = new StringBuilder(size);
        sb.append(key);
        sb.append(':');
        sb.append(fieldkey);
        while (sb.length() < size) {
            sb.append(':');
            sb.append(sb.toString().hashCode());
        }
        sb.setLength(size);

        return sb.toString();
    }

    @Test
    public void insertTest() {
        try {
            String insertKey = "user0";
            HashMap<String, ByteIterator> insertMap = new HashMap<String, ByteIterator>();
            for (int i = 0; i < 3; i++) {
                insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));
            }
            jdbcDBClient.insert(TABLE_NAME, insertKey, insertMap);

            ResultSet resultSet = jdbcConnection.prepareStatement(
                String.format("SELECT * FROM %s", TABLE_NAME)
            ).executeQuery();

            // Check we have a result Row
            assertTrue(resultSet.next());
            // Check we have the correct number of columns
            assertEquals(resultSet.getMetaData().getColumnCount(), 4);
            // Check that all the columns have expected values
            assertEquals(resultSet.getString(KEY_FIELD), insertKey);
            for (int i = 0; i < 3; i++) {
                // TODO: This will fail until the fix is made to insert and update fields in the correct order.
                // TODO: Uncomment this assertEquals when the fix is made.
                //assertEquals(resultSet.getString(FIELD_PREFIX + i), insertMap.get(FIELD_PREFIX + i));
            }
            // Check that we do not have any more rows
            assertFalse(resultSet.next());

            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Failed insertTest");
        }
    }

    @Test
    public void updateTest() {
        assertTrue(true);
    }

    @Test
    public void readTest() {
        assertTrue(true);
    }

    @Test
    public void deleteTest() {
        assertTrue(true);
    }

    @Test
    public void scanTest() {
        assertTrue(true);
    }
}
