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

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

public class KalDBSplitManager
        implements ConnectorSplitManager
{
    private final KalDBConfig config;

    @Inject
    public KalDBSplitManager(KalDBConfig config)
    {
        this.config = config;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KalDBTableHandle tableHandle = (KalDBTableHandle) table;
        //while a KalDB cluster can have multiple search nodes, it doesn't expose
        //the shard -> node mappings externally through a service. So we have to treat
        //it as being one large "shard" that we query from.
        List<KalDBSplit> splits = new ArrayList<KalDBSplit>(1);
        int port = config.getPort();
        splits.add(new KalDBSplit(tableHandle.getIndex(),
                config.getHosts().stream().map(s -> HostAddress.fromParts(s, port).toString()).findFirst()));
        return new FixedSplitSource(splits);
    }
}
