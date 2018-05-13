/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.session.Neo4JSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Factory for {@link Neo4JGraph}. Use the {@link Neo4JConfiguration#builder()} to build a new configuration for the factory.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JGraphFactory {

    @Nonnull
    public static Graph open(@Nonnull @NonNull final Neo4JConfiguration configuration) {
        final Driver driver = createDriver(configuration);
        final Neo4JSession session = new Neo4JSession(driver, configuration);
        return session.getGraph();
    }

    @Nonnull
    protected static Driver createDriver(@Nonnull @NonNull final Neo4JConfiguration configuration) {
        final Config config = createAdditionalConfiguration(configuration.getAdditionConfiguration());
        final String connectionString = "bolt://" + configuration.getHostname() + ":" + configuration.getPort();

        return GraphDatabase.driver(connectionString, config);
    }

    @Nonnull
    private static Config createAdditionalConfiguration(@Nullable final Consumer<Config.ConfigBuilder> consumer) {
        final Config.ConfigBuilder builder = Config.build();
        Optional.ofNullable(consumer).ifPresent(c -> c.accept(builder));
        return builder.toConfig();
    }

}
