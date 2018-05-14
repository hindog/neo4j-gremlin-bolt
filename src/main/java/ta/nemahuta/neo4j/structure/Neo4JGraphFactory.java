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
