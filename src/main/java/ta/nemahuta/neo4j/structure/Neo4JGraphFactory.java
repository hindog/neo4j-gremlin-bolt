package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import ta.nemahuta.neo4j.cache.JCacheSessionCacheManager;
import ta.nemahuta.neo4j.cache.SessionCacheManager;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Caching;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Factory for {@link Neo4JGraph}.
 * The factory holds a global cache for vertices and edges and the driver which provides a connection pool.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JGraphFactory implements AutoCloseable, Supplier<Graph> {

    private final SessionCacheManager cacheManager;
    private final Neo4JConfiguration configuration;
    private final Driver driver;

    public Neo4JGraphFactory(@Nonnull final Neo4JConfiguration configuration) {
        this(new JCacheSessionCacheManager(Caching.getCachingProvider(), configuration), configuration);
    }

    public Neo4JGraphFactory(@Nonnull final SessionCacheManager cacheManager,
                             @Nonnull final Neo4JConfiguration configuration) {
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
        return GraphDatabase.driver(connectionString, configuration.getAuthToken(), config);
    }

    @Nonnull
    private Config createAdditionalConfiguration(@Nullable final Consumer<Config.ConfigBuilder> consumer) {
        final Config.ConfigBuilder builder = Config.build();
        Optional.ofNullable(consumer).ifPresent(c -> c.accept(builder));
        return builder.toConfig();
    }

    @Override
    public void close() {
        driver.close();
        cacheManager.close();
    }

}
