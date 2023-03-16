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

import io.airlift.slice.Slice;
import io.trino.plugin.kaldb.decoders.Decoder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class KalDBRecordCursor
        implements RecordCursor
{
    private final KalDBTableHandle table;
    private final List<KalDBColumnHandle> columnHandles;
    private long completedBytes;
    private long readTimeNanos;

    private final SearchResponse response;
    private SearchHit currentHit;
    private int currentPos;
    private boolean isClosed;
    private Map<String, Object> currentDocument;
    private List<Object> fieldValues;
    private List<Decoder> decoders;

    public KalDBRecordCursor(SearchResponse response, KalDBTableHandle table, List<KalDBColumnHandle> columnHandles)
    {
        this.response = requireNonNull(response);
        this.table = requireNonNull(table);
        this.columnHandles = requireNonNull(columnHandles);
        this.decoders = createDecoders(columnHandles);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentPos++;
        if (isClosed || currentPos >= response.getHits().totalHits) {
            return false;
        }
        currentHit = response.getHits().getAt(currentPos);
        currentDocument = new TreeMap<>(currentHit.getSourceAsMap());
        fieldValues = currentDocument.values().stream().collect(Collectors.toUnmodifiableList());
        return true;
    }

    private void checkClosed()
    {
        if (isClosed) {
            throw new IllegalStateException("Can't operate on a closed result set!");
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BooleanType.BOOLEAN);
        return Boolean.parseBoolean(fieldValues.get(field).toString());
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BigintType.BIGINT);
        return Long.parseLong(fieldValues.get(field).toString());
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DoubleType.DOUBLE);
        return Double.parseDouble(fieldValues.get(field).toString());
    }

    @Override
    public Slice getSlice(int field)
    {
        return null;
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        isClosed = true;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private List<Decoder> createDecoders(List<KalDBColumnHandle> columns)
    {
        return columns.stream()
                .map(KalDBColumnHandle::getDecoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
    }
}
