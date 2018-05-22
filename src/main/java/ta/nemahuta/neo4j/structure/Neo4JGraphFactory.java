package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.cache.DefaultSessionCacheManager;
import ta.nemahuta.neo4j.cache.SessionCacheManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Factory for {@link Neo4JGraph}. Use the {@link Neo4JConfiguration#builder()} to build a new configuration for the factory.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JGraphFactory implements AutoCloseable, Supplier<Graph> {

    private final SessionCacheManager cacheManager;
    private final Neo4JConfiguration configuration;
    private final Driver driver;

    public Neo4JGraphFactory(@Nonnull @NonNull final Neo4JConfiguration configuration) {
        this(new DefaultSessionCacheManager(CacheManagerBuilder.newCacheManagerBuilder().build(true)), configuration);
    }

    public Neo4JGraphFactory(@Nonnull @NonNull final SessionCacheManager cacheManager,
                             @Nonnull @NonNull final Neo4JConfiguration configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.driver = createDriver();
    }

    @Nonnull
    @Override
    public Graph get() {
        return new Neo4JGraph(driver.session(), cacheManager, configuration);
    }

    @Nonnull
    protected Driver createDriver() {
        final Config config = createAdditionalConfiguration(configuration.getAdditionConfiguration());
        final String connectionString = "bolt://" + configuration.getHostname() + ":" + configuration.getPort();
        return GraphDatabase.driver(connectionString, config);
    }

    @Nonnull
    private Config createAdditionalConfiguration(@Nullable final Consumer<Config.ConfigBuilder> consumer) {
        final Config.ConfigBuilder builder = Config.build();
        Optional.ofNullable(consumer).ifPresent(c -> c.accept(builder));
        return builder.toConfig();
    }

    @Override
    public void close() throws Exception {
        driver.close();
        cacheManager.close();
    }

}
