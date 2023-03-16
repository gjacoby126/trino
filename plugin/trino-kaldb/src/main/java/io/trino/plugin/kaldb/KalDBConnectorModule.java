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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.kaldb.client.KalDBClient;
import io.trino.plugin.kaldb.ptf.RawQuery;
import io.trino.spi.ptf.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class KalDBConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(KalDBConnector.class).in(Scopes.SINGLETON);
        binder.bind(KalDBMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KalDBSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KalDBClient.class).in(Scopes.SINGLETON);
        binder.bind(KalDBRecordSetProvider.class).in(Scopes.SINGLETON);

        newExporter(binder).export(KalDBClient.class).withGeneratedName();

        configBinder(binder).bindConfig(KalDBConfig.class);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(RawQuery.class).in(Scopes.SINGLETON);
    }
}
