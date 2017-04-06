package org.apache.carbondata.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import org.apache.carbondata.presto.CarbondataPlugin;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class PrestoServerRunner {

  private static final Logger log = Logger.get(PrestoServerRunner.class);

  public static final String CARBONDATA_CATALOG = "carbondata";
  public static final String CARBONDATA_CONNECTOR = "carbondata";

  public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables,
      Map<String, String> extraProperties) throws Exception {
    DistributedQueryRunner queryRunner =
        new DistributedQueryRunner(createSession(), 4, extraProperties);

    try {
      queryRunner.installPlugin(new CarbondataPlugin());

      Map<String, String> carbonProperties = ImmutableMap.<String, String>builder()
          .put("carbondata-store", "hdfs://localhost:54311/carbonStore").build();
      queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties);
      return queryRunner;
    } catch (Exception e) {
      queryRunner.close();
      throw e;
    }
  }

  public static Session createSession() {
    return Session.builder(new SessionPropertyManager())
        .setQueryId(new QueryIdGenerator().createNextQueryId())
        .setIdentity(new Identity("user", Optional.empty())).setSource("carbondata")
        .setCatalog(CARBONDATA_CATALOG).setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
        .setRemoteUserAddress("address").setUserAgent("agent").build();
  }

  public static void main(String[] args) throws Exception {
    // You need to add "--user user" to your CLI for your queries to work
    Logging.initialize();
    log.info("======== STARTING PRESTO SERVER ========");
    DistributedQueryRunner queryRunner =
        createQueryRunner(TpchTable.getTables(), ImmutableMap.of("http-server.http.port", "8086"));
    Thread.sleep(10);
    Logger log = Logger.get(DistributedQueryRunner.class);
    log.info("======== SERVER STARTED ========");
    log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

    //Format for JDBC_DRIVER = jdbc:presto://host:port/catalog/schema
    final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    final String DB_URL = "jdbc:presto://localhost:8086/carbondata/newstore";

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
      //STEP 4: Execute a query
      stmt = conn.createStatement();
      String sql = "select * from user";
      ;
      ResultSet res = stmt.executeQuery(sql);
      //STEP 5: Extract data from result set
      while (res.next()) {
        //Retrieve by column name
        int id = res.getInt("id");
        String name = res.getString("name");
        //Display values
        System.out.println("id: " + id + "\n name : " + name);
      }
      //STEP 6: Clean-up environment
      res.close();
      System.out.println("Query executed successfully !!");
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

}



