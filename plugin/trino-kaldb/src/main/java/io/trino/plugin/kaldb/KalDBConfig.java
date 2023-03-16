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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KalDBConfig
{
    private List<String> hosts;

    private String dataSetHost;
    private int port = 8081;

    private int dataSetPort = 8083;
    private String defaultSchema = "default";
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, MINUTES);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private Duration connectTimeout = new Duration(1, SECONDS);
    private Duration backoffInitDelay = new Duration(500, MILLISECONDS);
    private Duration backoffMaxDelay = new Duration(20, SECONDS);
    private Duration maxRetryTime = new Duration(30, SECONDS);
    private int maxHttpConnections = 25;
    private int httpThreadCount = Runtime.getRuntime().availableProcessors();
    private boolean verifyHostnames = true;

    @NotNull
    public List<String> getHosts()
    {
        return hosts;
    }

    @Config("kaldb.host")
    public KalDBConfig setHosts(List<String> hosts)
    {
        this.hosts = hosts;
        return this;
    }

    public int getDataSetPort()
    {
        return dataSetPort;
    }

    @Config("kaldb.dataset.port")
    public KalDBConfig setDataSetPort(int dataSetPort)
    {
        this.dataSetPort = dataSetPort;
        return this;
    }

    public String getDataSetHost()
    {
        return dataSetHost;
    }

    @Config("kaldb.dataset.host")
    public KalDBConfig setDataSetHost(String host)
    {
        this.dataSetHost = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("kaldb.port")
    public KalDBConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kaldb.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public KalDBConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    @Min(1)
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("kaldb.scroll-size")
    @ConfigDescription("Scroll batch size")
    public KalDBConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("kaldb.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public KalDBConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("kaldb.request-timeout")
    @ConfigDescription("kaldb request timeout")
    public KalDBConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("kaldb.connect-timeout")
    @ConfigDescription("kaldb connect timeout")
    public KalDBConfig setConnectTimeout(Duration timeout)
    {
        this.connectTimeout = timeout;
        return this;
    }

    @NotNull
    public Duration getBackoffInitDelay()
    {
        return backoffInitDelay;
    }

    @Config("kaldb.backoff-init-delay")
    @ConfigDescription("Initial delay to wait between backpressure retries")
    public KalDBConfig setBackoffInitDelay(Duration backoffInitDelay)
    {
        this.backoffInitDelay = backoffInitDelay;
        return this;
    }

    @NotNull
    public Duration getBackoffMaxDelay()
    {
        return backoffMaxDelay;
    }

    @Config("kaldb.backoff-max-delay")
    @ConfigDescription("Maximum delay to wait between backpressure retries")
    public KalDBConfig setBackoffMaxDelay(Duration backoffMaxDelay)
    {
        this.backoffMaxDelay = backoffMaxDelay;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("kaldb.max-retry-time")
    @ConfigDescription("Maximum timeout in case of multiple retries")
    public KalDBConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @Config("kaldb.max-http-connections")
    @ConfigDescription("Maximum number of persistent HTTP connections to kaldb")
    public KalDBConfig setMaxHttpConnections(int size)
    {
        this.maxHttpConnections = size;
        return this;
    }

    @NotNull
    public int getMaxHttpConnections()
    {
        return maxHttpConnections;
    }

    @Config("kaldb.http-thread-count")
    @ConfigDescription("Number of threads handling HTTP connections to kaldb")
    public KalDBConfig setHttpThreadCount(int count)
    {
        this.httpThreadCount = count;
        return this;
    }

    @NotNull
    public int getHttpThreadCount()
    {
        return httpThreadCount;
    }
}
