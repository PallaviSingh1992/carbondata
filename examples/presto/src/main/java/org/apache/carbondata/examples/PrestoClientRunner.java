package org.apache.carbondata.examples;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import java.sql.*;

public class PrestoClientRunner {

    private static final Logger log = Logger.get(PrestoClientRunner.class);

    /**
     * Creates a JDBC Client to connect CarbonData to PrestoDbgit
     * @throws Exception
     */
    public static void prestoJdbcClient() throws Exception {

        Logging.initialize();
        log.info("======== STARTING PRESTO SERVER ========");
        DistributedQueryRunner queryRunner =
                PrestoServerRunner.createQueryRunner(ImmutableMap.of("http-server.http.port", "8086"));
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("========STARTED SERVER ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

        /**
         * The Format for Presto JDBC Driver is :
         * jdbc:presto://<host>:<port>/<catalog>/<schema>
         */
        //Step 1: Create Connection Strings
        final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
        final String DB_URL = "jdbc:presto://localhost:8086/carbondata/demo";

        /**
         * The database Credentials
         */
        final String USER = "username";
        final String PASS = "password";

        Connection conn = null;
        Statement stmt = null;
        try {
            log.info("=============Connecting to database/table : demo/uniqdata ===============");
            //STEP 2: Register JDBC driver
            Class.forName(JDBC_DRIVER);
            //STEP 3: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            //STEP 4: Execute a query
            stmt = conn.createStatement();
            String sql = "select * from uniqdata_date_column_withoutnull";

            ResultSet res = stmt.executeQuery(sql);
            //STEP 5: Extract data from result set
            while (res.next()) {
                //Retrieve by column name
                int id = res.getInt("cust_id");
                String name = res.getString("cust_name");
                //Display values
                System.out.println("id: " + id + "\n name : " + name);
            }
            //STEP 6: Clean-up environment
            res.close();
            log.info("Query executed successfully !!");
            //STEP 7: Close the Connection
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }


    }

    public static void main(String[] args) throws Exception {
        prestoJdbcClient();
    }
}
