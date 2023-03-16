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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KalDBLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final KafkaProducer<String, byte[]> producer;
    private final String tableName;

    public KalDBLoader(KafkaProducer<String, byte[]> producer,
            String tableName,
            TestingTrinoServer trinoServer,
            Session defaultSession)
    {
        super(trinoServer, defaultSession);
        this.producer = producer;
        this.tableName = tableName;
    }

    @Override
    protected ResultsSession<Void> getResultSession(Session session)
    {
        return new KalDBLoadingSession();
    }

    private class KalDBLoadingSession
            implements ResultsSession<Void>
    {
        private final ObjectMapper mapper = new ObjectMapper();
        private long rowId;

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            List<Column> columns = statusInfo.getColumns();

            Map<String, Object> fieldMap = new HashMap<>();
            for (List<Object> fields : data.getData()) {
                try {
                    for (int k = 0; k < fields.size(); k++) {
                        fieldMap.put(columns.get(k).getName(), fields.get(k));
                    }
                    byte[] jsonPayload = mapper.writeValueAsString(fieldMap).getBytes(StandardCharsets.UTF_8);
                    producer.send(new ProducerRecord<>(statusInfo.getId() + "_" +
                            Long.toString(rowId++), jsonPayload));
                }
                catch (JsonProcessingException e) {
                    throw new UncheckedIOException("Error creating JSON for loading into KalDB", e);
                }
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }
    }
}
