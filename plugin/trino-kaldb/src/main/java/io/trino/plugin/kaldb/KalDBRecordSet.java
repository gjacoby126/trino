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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KalDBRecordSet
        implements RecordSet
{
    private final SearchResponse response;
    private final KalDBTableHandle table;
    private final List<KalDBColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private static final Logger LOG = Logger.get(KalDBRecordSet.class);

    public KalDBRecordSet(SearchResponse response, KalDBSplit split, KalDBTableHandle table,
            List<KalDBColumnHandle> columnHandles)
    {
        this.response = requireNonNull(response, "SearchResponse is null");
        requireNonNull(split, "split is null");
        this.table = requireNonNull(table);
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KalDBColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();
        LOG.info("Column types for record set are: " + columnTypes);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        LOG.info("Creating a cursor based on response [" + response + "]");
        return new KalDBRecordCursor(response, table, columnHandles);
    }
}
