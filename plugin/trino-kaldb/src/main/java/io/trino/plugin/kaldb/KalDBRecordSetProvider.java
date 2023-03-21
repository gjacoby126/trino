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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.kaldb.client.KalDBClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KalDBRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger LOG = Logger.get(KalDBRecordSetProvider.class);

    private KalDBClient client;

    @Inject
    public KalDBRecordSetProvider(KalDBClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        KalDBSplit kalDBSplit = (KalDBSplit) split;
        KalDBTableHandle kalDBTable = (KalDBTableHandle) table;
        ImmutableList.Builder<KalDBColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((KalDBColumnHandle) handle);
        }
        List<KalDBColumnHandle> kalDBColumnHandles = handles.build();
        QueryBuilder query = KalDBQueryBuilder.buildSearchQuery(
                kalDBTable.getConstraint().transformKeys(KalDBColumnHandle.class::cast),
                ((KalDBTableHandle) table).getQuery(), ((KalDBTableHandle) table).getRegexes());
        String index = ((KalDBTableHandle) table).getIndex();
        List<String> fields = kalDBColumnHandles.stream().map(KalDBColumnHandle::getName).collect(Collectors.toList());
        List<String> docFields = List.of();
        LOG.info("Columns to query are: " + fields);
        SearchResponse response = client.beginSearch(index, query, Optional.of(fields), docFields,
                Optional.empty(), kalDBTable.getLimit());
        LOG.info("Search response is: " + response);
        return new KalDBRecordSet(response, kalDBSplit, kalDBTable, kalDBColumnHandles);
    }
}
