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
package io.trino.plugin.kaldb.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.manager_api.ManagerApi;
import com.slack.kaldb.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.kaldb.proto.metadata.Metadata;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.trino.plugin.kaldb.KalDBConfig;
import io.trino.spi.TrinoException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.kaldb.KalDBErrorCode.KALDB_CONNECTION_ERROR;
import static io.trino.plugin.kaldb.KalDBErrorCode.KALDB_QUERY_FAILURE;
import static java.lang.StrictMath.toIntExact;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class KalDBClient
{
    private static final Logger LOG = Logger.get(KalDBClient.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");
    public static final int KALDB_MAX_LIMIT = 10;

    private final BackpressureRestHighLevelClient client;

    private final ManagerApiServiceGrpc.ManagerApiServiceBlockingStub dataSetStub;

    private ManagedChannel dataSetChannel;
    private final int scrollSize;
    private final Duration scrollTimeout;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final TimeStat searchStats = new TimeStat(MILLISECONDS);
    private final TimeStat nextPageStats = new TimeStat(MILLISECONDS);
    private final TimeStat countStats = new TimeStat(MILLISECONDS);
    private final TimeStat backpressureStats = new TimeStat(MILLISECONDS);

    @Inject
    public KalDBClient(
            KalDBConfig config)
    {
        client = createClient(config, backpressureStats);
        //TODO: switch to use TLS -- using Plaintext HTTP for testing
        dataSetChannel = ManagedChannelBuilder.forAddress(config.getDataSetHost(),
                config.getDataSetPort()).usePlaintext().build();
        dataSetStub = ManagerApiServiceGrpc.newBlockingStub(dataSetChannel);
        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
    }

    @PostConstruct
    public void initialize()
    {}

    @PreDestroy
    public void close()
            throws IOException
    {
        executor.shutdownNow();
        client.close();
        dataSetChannel.shutdownNow();
    }

    private static BackpressureRestHighLevelClient createClient(
            KalDBConfig config,
            TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                        config.getHosts().stream()
                                .map(httpHost -> new HttpHost(httpHost, config.getPort(), "http"))
                                .toArray(HttpHost[]::new))
                .setMaxRetryTimeoutMillis(toIntExact(config.getMaxRetryTime().toMillis()));

        builder.setHttpClientConfigCallback(ignored -> {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(toIntExact(config.getConnectTimeout().toMillis()))
                    .setSocketTimeout(toIntExact(config.getRequestTimeout().toMillis()))
                    .build();

            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            // the client builder passed to the call-back is configured to use system properties, which makes it
            // impossible to configure concurrency settings, so we need to build a new one from scratch
            HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultIOReactorConfig(reactorConfig)
                    .setMaxConnPerRoute(config.getMaxHttpConnections())
                    .setMaxConnTotal(config.getMaxHttpConnections());
            //TODO: implement TLS and password auth with KalDB -- below is from ES
            /*
            if (config.isTlsEnabled()) {
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword())
                        .ifPresent(clientBuilder::setSSLContext);

                if (config.isVerifyHostnames()) {
                    clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
            }

            passwordConfig.ifPresent(securityConfig -> {
                CredentialsProvider credentials = new BasicCredentialsProvider();
                credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(securityConfig.getUser(), securityConfig.getPassword()));
                clientBuilder.setDefaultCredentialsProvider(credentials);
            });

            awsSecurityConfig.ifPresent(securityConfig -> clientBuilder.addInterceptorLast(new AwsRequestSigner(
                    securityConfig.getRegion(),
                    getAwsCredentialsProvider(securityConfig))));
            */
            return clientBuilder;
        });

        return new BackpressureRestHighLevelClient(builder, config, backpressureStats);
    }

    public boolean indexExists(String index)
    {
        //TODO: replace with call to KalDB manager REST call for dataset

        Metadata.DatasetMetadata indexMetadata =
                    dataSetStub.getDatasetMetadata(
                            ManagerApi.GetDatasetMetadataRequest
                                    .newBuilder().setName(index).build());
        return (indexMetadata != null && indexMetadata.getName().equals(index));
    }

    public List<String> getIndexes()
    {
        ManagerApi.ListDatasetMetadataResponse listResponse =
                dataSetStub.listDatasetMetadata(
                        ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
        List<Metadata.DatasetMetadata> dataSetProtoList = listResponse.getDatasetMetadataList();
        return dataSetProtoList.stream().map(Metadata.DatasetMetadata::getName).collect(Collectors.toList());
    }

    public IndexMetadata getIndexMetadata(String index)
    {
        return getMetadataFromFixedSchema(index);
       // return getMetadataFromWildcardQuery(index);
    }

    private IndexMetadata getMetadataFromFixedSchema(String index)
    {
        List<IndexMetadata.Field> fields = new ArrayList<>(5);
//        fields.add(new IndexMetadata.Field(false, false, "id",
//                new IndexMetadata.PrimitiveType("keyword")));
        fields.add(new IndexMetadata.Field(false, false, "duration_ms",
                new IndexMetadata.PrimitiveType("long")));
        fields.add(new IndexMetadata.Field(false, false, "trace_id",
                new IndexMetadata.PrimitiveType("keyword")));
        fields.add(new IndexMetadata.Field(false, false, "@timestamp",
                new IndexMetadata.PrimitiveType("long")));
        fields.add(new IndexMetadata.Field(false, false, "mf_errorMessage",
                new IndexMetadata.PrimitiveType("text")));
        IndexMetadata.ObjectType schemaTypes = new IndexMetadata.ObjectType(fields);
        return new IndexMetadata(schemaTypes);
    }

    private IndexMetadata getMetadataFromWildcardQuery(String index)
    {
        //TODO: implement with KalDB's dataset and search APIs
        //FIXME:
        // We send a dummy *.* query with limit 1 to the search API and use the search
        // result to infer the index's schema. We have to do it this way because
        // KalDB doesn't (yet) implement the Elastic _mappings API. We set the timestamp
        // to anytime between 2020 and 2030 because KalDB requires a timestamp filter
        // for every query.
        RangeQueryBuilder queryBuilder =
                new RangeQueryBuilder(LogMessage.ReservedField.TIMESTAMP.fieldName);
        queryBuilder.gte(
                Instant.from(ZonedDateTime.of(
                                LocalDate.of(2020, 1, 1),
                                LocalTime.MIDNIGHT,
                                ZoneId.of("UTC")))
                        .getEpochSecond() * 1000);
        queryBuilder.lte(
                Instant.from(ZonedDateTime.of(
                        LocalDate.of(2030, 12, 31),
                        LocalTime.MIDNIGHT,
                        ZoneId.of("UTC")))
                        .getEpochSecond() * 1000);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //get the source document with all fields -- we assume here that every
        //doc has every field!
        searchSourceBuilder.fetchSource(true);
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(1);
        SearchResponse response = getSearchResponse(index, searchSourceBuilder);
        if (response.getHits().totalHits < 1) {
            throw new TrinoException(KALDB_QUERY_FAILURE, "Found no hits for auto-detection of fields " + response.toString());
        }
        SearchHit hit = response.getHits().getAt(0);
        //put it in a TreeMap to guarantee a consistent order of fields
        Map<String, Object> fieldMap = new TreeMap<>(hit.getSourceAsMap());
        List<IndexMetadata.Field> fields = new ArrayList<>(fieldMap.size());
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            //TODO: support non-text fields for fields other than @timestamp
            IndexMetadata.Type type;
            if (entry.getKey().equals("@timestamp")) {
                type = new IndexMetadata.DateTimeType(ImmutableList.of());
            }
            else {
                type = new IndexMetadata.PrimitiveType("text");
            }
            IndexMetadata.Field field = new IndexMetadata.Field(false, false, entry.getKey(), type);
            fields.add(field);
        }

        IndexMetadata.ObjectType schemaTypes = new IndexMetadata.ObjectType(fields);
        return new IndexMetadata(schemaTypes);
    }

    public SearchResponse beginSearch(String index, QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, Optional<String> sort, OptionalLong limit)
    {
        SearchSourceBuilder sourceBuilder = getSearchSourceBuilder(query, fields, documentFields, sort, limit);
        return getSearchResponse(index, sourceBuilder);
    }

    private SearchResponse getSearchResponse(String index, SearchSourceBuilder sourceBuilder)
    {
        LOG.debug("Begin search: %s, query: %s", index, sourceBuilder);
        //we only want to do a single query. But KalDB only emulates ElasticSearch's
        //"msearch", or multiple query REST API. So we have to use the multi-query
        //API and just take the first result.
        SearchRequest request = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                .scroll(new TimeValue(scrollTimeout.toMillis()))
                .source(sourceBuilder);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(request);
        long start = System.nanoTime();
        try {
            MultiSearchResponse multiSearchResponse = client.msearch(multiSearchRequest);
            if (multiSearchResponse.getResponses().length > 0) {
                MultiSearchResponse.Item item = multiSearchResponse.getResponses()[0];
                if (item.isFailure()) {
                    throw item.getFailure();
                }
                else {
                    return item.getResponse();
                }
            }
            else {
                throw new TrinoException(KALDB_QUERY_FAILURE, "The query failed to return any results");
            }
        }
        catch (IOException e) {
            throw new TrinoException(KALDB_QUERY_FAILURE, e);
        }
        catch (Exception e) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed.length > 0) {
                Throwable cause = suppressed[0];
                if (cause instanceof ResponseException) {
                    throw propagate((ResponseException) cause);
                }
            }
            throw new TrinoException(KALDB_CONNECTION_ERROR, e);
        }
        finally {
            searchStats.add(Duration.nanosSince(start));
        }
    }

    private static SearchSourceBuilder getSearchSourceBuilder(QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, Optional<String> sort, OptionalLong limit)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query);
        // Safe to cast it to int because scrollSize is int.
        if (limit != null && limit.isPresent()) {
            sourceBuilder.size(toIntExact(limit.getAsLong()));
        }
        else {
            sourceBuilder.size(KALDB_MAX_LIMIT);
        }
        sort.ifPresent(sourceBuilder::sort);

        fields.ifPresent(values -> {
            if (values.isEmpty()) {
                sourceBuilder.fetchSource(false);
            }
            else {
                sourceBuilder.fetchSource(values.toArray(new String[0]), null);
            }
        });
        documentFields.forEach(sourceBuilder::docValueField);
        return sourceBuilder;
    }

    @Managed
    @Nested
    public TimeStat getSearchStats()
    {
        return searchStats;
    }

    @Managed
    @Nested
    public TimeStat getNextPageStats()
    {
        return nextPageStats;
    }

    @Managed
    @Nested
    public TimeStat getCountStats()
    {
        return countStats;
    }

    @Managed
    @Nested
    public TimeStat getBackpressureStats()
    {
        return backpressureStats;
    }

    private static TrinoException propagate(ResponseException exception)
    {
        HttpEntity entity = exception.getResponse().getEntity();

        if (entity != null && entity.getContentType() != null) {
            try {
                JsonNode reason = OBJECT_MAPPER.readTree(entity.getContent()).path("error")
                        .path("root_cause")
                        .path(0)
                        .path("reason");

                if (!reason.isMissingNode()) {
                    throw new TrinoException(KALDB_QUERY_FAILURE, reason.asText(), exception);
                }
            }
            catch (IOException e) {
                TrinoException result = new TrinoException(KALDB_QUERY_FAILURE, exception);
                result.addSuppressed(e);
                throw result;
            }
        }

        throw new TrinoException(KALDB_QUERY_FAILURE, exception);
    }

    @VisibleForTesting
    static Optional<String> extractAddress(String address)
    {
        Matcher matcher = ADDRESS_PATTERN.matcher(address);

        if (!matcher.matches()) {
            return Optional.empty();
        }

        String cname = matcher.group("cname");
        String ip = matcher.group("ip");
        String port = matcher.group("port");

        if (cname != null) {
            return Optional.of(cname + ":" + port);
        }

        return Optional.of(ip + ":" + port);
    }

    private interface ResponseHandler<T>
    {
        T process(String body);
    }
}
