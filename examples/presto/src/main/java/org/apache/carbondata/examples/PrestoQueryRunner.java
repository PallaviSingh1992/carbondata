package org.apache.carbondata.examples;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import org.apache.carbondata.presto.CarbondataPlugin;
import org.apache.carbondata.presto.impl.CarbonTableConfig;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class PrestoQueryRunner {
    private static final Logger log = Logger.get(PrestoQueryRunner.class);

    public static final String CARBONDATA_CATALOG = "carbondata";
    public static final String CARBONDATA_SCHEMA = "demo";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    public static DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(tables, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(tables, extraProperties, "sql-standard", ImmutableMap.of());
    }


    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties,String security, Map<String, String> extraCarbonProperties) throws Exception {
        Map<String, String> carbondataProperties = ImmutableMap.<String, String>builder()
                .putAll(extraProperties)
                .put("connector.name","carbondata")
                .put("carbondata-store", "hdfs://192.168.2.130:54311/carbonStore")
                .put("datasources","carbondata")
                .put("catalog.config-dir","/")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4 ,carbondataProperties);
        try {

            CarbonTableConfig carbonConfig=new CarbonTableConfig();
            carbonConfig.setStorePath("hdfs://192.168.2.130:54311/carbonStore");
            carbonConfig.setDbPath("hdfs://192.168.2.130:54311/carbonStore");
            carbonConfig.setTablePath("hdfs://192.168.2.130:54311/carbonStore");

            queryRunner.installPlugin(new CarbondataPlugin());
            queryRunner.createCatalog("carbondata","carbondata");

            queryRunner.createCatalog(CARBONDATA_CATALOG,CARBONDATA_CATALOG,carbondataProperties);

            return queryRunner;

        } catch (Exception e) {
            queryRunner.close();
            e.printStackTrace();
            throw e;
        }
    }


    public static Session createSession()
    {
        return Session.builder(new SessionPropertyManager())
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource(CARBONDATA_CATALOG)
                .setCatalog(CARBONDATA_CATALOG)
                .setSchema(CARBONDATA_SCHEMA)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();

      /*  return testSessionBuilder()
                .setCatalog(CARBONDATA_CATALOG)
                .setSchema(CARBONDATA_SCHEMA)
                .build();*/
    }

    public static void main(String[] args) throws Exception {
        Logging.initialize();
       /* DistributedQueryRunner queryRunner=createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"),new HashMap<String,String>());*/
        DistributedQueryRunner queryRunner=createQueryRunner(TpchTable.getTables(),ImmutableMap.of("http-server.http.port", "8080"));
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");

    }
}
