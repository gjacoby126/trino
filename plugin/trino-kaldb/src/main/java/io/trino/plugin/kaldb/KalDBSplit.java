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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;

public class KalDBSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(KalDBSplit.class).instanceSize());

    private final String index;
    private Optional<String> address;

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    public Optional<String> getAddress()
    {
        return address;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return address.map(host -> ImmutableList.of(HostAddress.fromString(host)))
                .orElseGet(ImmutableList::of);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(index)
                + sizeOf(address, SizeOf::estimatedSizeOf);
    }

    @JsonCreator
    public KalDBSplit(
            @JsonProperty("index") String index,
            @JsonProperty("hostAddresses") Optional<String> address)
    {
        this.index = index;
        this.address = address;
    }
}
