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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.kaldb.decoders.Decoder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
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
    private int currentPos = -1;
    private boolean isClosed;
    private Map<String, Object> currentDocument;
    private List<List<Object>> fieldValueRows = new ArrayList<>();
    private List<Decoder> decoders;
    private final int[] fieldToColumnIndex;

    private final BlockBuilder[] columnBuilders;
    private static final Logger LOG = Logger.get(KalDBRecordCursor.class);

    public KalDBRecordCursor(SearchResponse response, KalDBTableHandle table, List<KalDBColumnHandle> columnHandles)
    {
        this.response = requireNonNull(response);
        this.table = requireNonNull(table);
        this.columnHandles = requireNonNull(columnHandles);
        this.decoders = createDecoders(columnHandles);
        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            KalDBColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
        LOG.info("columnHandles: " + columnHandles);
        LOG.info("fieldToColumnIndex: " + Arrays.toString(fieldToColumnIndex));
        columnBuilders = columnHandles.stream()
                .map(KalDBColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
        for (SearchHit hit : response.getHits()) {
            Map<String, Object> document = new LinkedHashMap<>(hit.getSourceAsMap());
            fieldValueRows.add(document.values().stream().collect(Collectors.toUnmodifiableList()));
            /*
            for (int i = 0; i < decoders.size(); i++) {
                String field = columnHandles.get(i).getName();
                decoders.get(i).decode(hit, () -> getField(document, field), columnBuilders[i]);
            }
             */
        }
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
        LOG.info("advanceNextPosition called");
        // response.getHits().getTotalHits() refers to the sum of hits for the query
        // across pages, and may be an approximation. To get the number of hits returned on _this_
        // request, check the SearchHits array length directly
        int numHits = response.getHits().getHits().length;
        if (numHits == 0 || isClosed || currentPos + 1 >= numHits) {
            LOG.info("advanceNextPosition returning false: " + isClosed + "/" + currentPos);
            return false;
        }
        currentPos++;

        LOG.info("Advanced to hit [" + currentPos + "]");
        LOG.info("Current field value row is: " + fieldValueRows.get(currentPos));
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
        return Boolean.parseBoolean(getField(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BigintType.BIGINT);
        //@timestamp is a special field in KalDB that we query like a long
        //but get a response in a string-ified date
        LOG.info("Parsing field " + field + " which is column handle " + columnHandles.get(field));
        if (columnHandles.get(field).getName().equals("@timestamp")) {
            return ZonedDateTime.parse(getField(field),
                    DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"))).toInstant().toEpochMilli();
        }
        else {
            return Long.parseLong(getField(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DoubleType.DOUBLE);
        return Double.parseDouble(getField(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getField(field));
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

    private String getField(int field)
    {
        return fieldValueRows.get(currentPos)
                .get(fieldToColumnIndex[field]).toString();
    }

    private String getField(Map<String, Object> document, String field)
    {
        return document.get(field).toString();
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
