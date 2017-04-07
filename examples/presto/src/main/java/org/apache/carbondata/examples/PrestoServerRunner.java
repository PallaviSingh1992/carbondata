package org.apache.carbondata.examples;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.carbondata.presto.CarbondataPlugin;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class PrestoServerRunner {

    private static final Logger log = Logger.get(PrestoServerRunner.class);

    /**
     * CARBONDATA_CATALOG : stores the name of the catalog in the etc/catalog for Apache CarbonData.
     * CARBONDATA_CONNECTOR : stores the name of the CarbonData connector for Presto.
     * The etc/catalog will contain a catalog file for CarbonData with name <CARBONDATA_CATALOG>.properties
     * and following content :
     * connector.name = <CARBONDATA_CONNECTOR>
     * carbondata-store = <CARBONDATA_STOREPATH>
     */

    public static final String CARBONDATA_CATALOG = "carbondata";
    public static final String CARBONDATA_CONNECTOR = "carbondata";
    public static final String CARBONDATA_STOREPATH = "hdfs://localhost:54310/user/hive/warehouse/carbon.store";
    public static final String CARBONDATA_SOURCE = "carbondata";

    /**
     * Instantiates the Presto Server to connect with the Apache CarbonData
     *
     * @param extraProperties
     * @return Instance of running server.
     * @throws Exception
     */

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties) throws Exception {
        DistributedQueryRunner queryRunner =
                new DistributedQueryRunner(createSession(), 4, extraProperties);

        try {
            queryRunner.installPlugin(new CarbondataPlugin());

            Map<String, String> carbonProperties = ImmutableMap.<String, String>builder()
                    .put("carbondata-store", CARBONDATA_STOREPATH).build();

            /**
             * createCatalog will create a catalog for CarbonData in etc/catalog. It takes following parameters
             * CARBONDATA_CATALOG : catalog name
             * CARBONDATA_CONNECTOR : connector name
             * carbonProperties : Map of properties to be configured for CarbonData Connector.
             */
            queryRunner.createCatalog(CARBONDATA_CATALOG, CARBONDATA_CONNECTOR, carbonProperties);
            return queryRunner;
        } catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    /**
     * createSession will create a new session in the Server to connect and execute queries.
     *
     * @return a Session instance
     */
    public static Session createSession() {
        return Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryIdGenerator().createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty())).setSource(CARBONDATA_SOURCE)
                .setCatalog(CARBONDATA_CATALOG).setTimeZoneKey(UTC_KEY).setLocale(ENGLISH)
                .setRemoteUserAddress("address").setUserAgent("agent").build();
    }

}



