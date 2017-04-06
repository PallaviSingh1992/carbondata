package org.apache.carbondata.examples;

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import org.apache.carbondata.presto.CarbondataPlugin;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.sql.*;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class PrestoServerRunner {

    private static final Logger log = Logger.get(PrestoServerRunner.class);

    public static final String CARBONDATA_CATALOG = "carbondata";
    public static final String CARBONDATA_CONNECTOR="carbondata";
    public static final String TPCH_SCHEMA = "demo";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("Asia/Kolkata");

   /* public static DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }*/

   /* public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception {
        return createQueryRunner(tables, ImmutableMap.of());
    }*/

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties)
            throws Exception {
        return createQueryRunner(tables, extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties, Map<String, String> extraHiveProperties)
            throws Exception {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4, extraProperties);

        try {

            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("carbondata_data").toFile();
            queryRunner.installPlugin(new CarbondataPlugin());

            Map<String, String> carbonProperties = ImmutableMap.<String, String>builder()
                    .putAll(extraHiveProperties)
                    .put("carbondata-store", "hdfs://localhost:54310/user/hive/warehouse/carbon.store")
                    .build();
            queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties);
            return queryRunner;
        } catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static Session createSession() {
        return testSessionBuilder()
                .setCatalog(CARBONDATA_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception {
        // You need to add "--user user" to your CLI for your queries to work
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(TpchTable.getTables(), ImmutableMap.of("http-server.http.port", "8086"));
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

        final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
        final String DB_URL = "jdbc:presto://localhost:8086/carbondata/demo";
        //  Database credentials
        final String USER = "username";
        final String PASS = "password";
        Connection conn = null;
        Statement stmt = null;
        try {
            //STEP 2: Register JDBC driver
            Class.forName(JDBC_DRIVER);
            //STEP 3: Open a connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            //conn.setCatalog("hive");
            //STEP 4: Execute a query
            stmt = conn.createStatement();
            String sql;
            sql = "show schemas";
            ResultSet res = stmt.executeQuery(sql);
            //STEP 5: Extract data from result set
          /*  while (res.next()) {
                //Retrieve by column name
                //int id = res.getInt("");
                String name = res.getString("name");
                //Display values
                System.out.println(*//*"id: " + id +*//* "\n name : " + name);
            }*/
            //STEP 6: Clean-up environment
            // rs.close();
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
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    }



