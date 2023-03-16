/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kaldb;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KalDBQueryRunner
{
    private KalDBQueryRunner() {}

    private static final Logger LOG = Logger.get(KalDBQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createKalDBQueryRunner(
            HostAndPort address,
            HostAndPort dataSetAddress,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            int nodeCount)
            throws Exception
    {
        return createKalDBQueryRunner(address, dataSetAddress, tables, extraProperties, extraConnectorProperties, nodeCount, "elasticsearch");
    }

    public static DistributedQueryRunner createKalDBQueryRunner(
            HostAndPort address,
            HostAndPort dataSetAddress,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            int nodeCount,
            String catalogName)
            throws Exception
    {
        RestHighLevelClient client = null;
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                            .setCatalog(catalogName)
                            .setSchema(TPCH_SCHEMA)
                            .build())
                    .setExtraProperties(extraProperties)
                    .setNodeCount(nodeCount)
                    .build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            KalDBConnectorFactory testFactory = new KalDBConnectorFactory();

            installKalDBPlugin(address, dataSetAddress, queryRunner, catalogName, testFactory, extraConnectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();

            LOG.info("Loading data...");

            client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address.toString())));
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(trinoClient, table);
            }
            LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, client);
            throw e;
        }
    }

    private static void installKalDBPlugin(
            HostAndPort address,
            HostAndPort dataSetAddress,
            QueryRunner queryRunner,
            String catalogName,
            KalDBConnectorFactory factory,
            Map<String, String> extraConnectorProperties)
    {
        queryRunner.installPlugin(new KalDBPlugin(factory));
        Map<String, String> properties = getConfigProperties(address, dataSetAddress, extraConnectorProperties);
        queryRunner.createCatalog(catalogName, "kaldb", properties);
    }

    private static Map<String, String> getConfigProperties(HostAndPort address,
            HostAndPort dataSetAddress,
            Map<String, String> extraConnectorProperties)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("kaldb.host", address.getHost())
                .put("kaldb.port", Integer.toString(address.getPort()))
                .put("kaldb.dataset.host", dataSetAddress.getHost())
                .put("kaldb.dataset.port", Integer.toString(dataSetAddress.getPort()))
                .put("kaldb.default-schema-name", TPCH_SCHEMA)
                .put("kaldb.scroll-size", "1000")
                .put("kaldb.scroll-timeout", "1m")
                .put("kaldb.request-timeout", "2m")
                .putAll(extraConnectorProperties)
                .buildOrThrow();
        return config;
    }

    private static void loadTpchTopic(TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        try (KafkaProducer<String, byte[]> producer = getKafkaProducer()) {
            KalDBLoader loader = new KalDBLoader(producer, table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
            loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
            LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
        }
    }

    private static KafkaProducer<String, byte[]> getKafkaProducer()
    {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "10");
        kafkaConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "trino-kaldb-query-loader");
        kafkaConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500");
        return new KafkaProducer<>(kafkaConfig);
    }

    public static void main(String[] args)
            throws Exception
    {
        KalDBConfig config = new KalDBConfig();

        DistributedQueryRunner queryRunner = createKalDBQueryRunner(
                HostAndPort.fromParts("localhost", 8081),
                HostAndPort.fromParts("localhost", 8083),
                TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                3);

        Logger log = Logger.get(KalDBQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
