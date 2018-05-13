package ta.nemahuta.neo4j.config;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

@Builder
public class Neo4JConfiguration {

    public static final Function<Driver, Neo4JElementIdAdapter<?>> DEFAULT_ID_ADAPTER_FACTORY = driver -> new Neo4JNativeElementIdAdapter();

    /**
     * the factory for the {@link Neo4JElementIdAdapter} for {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es
     */
    private final Function<Driver, Neo4JElementIdAdapter<?>> vertexIdAdapterFactory;

    /**
     * the factory for the {@link Neo4JElementIdAdapter} for {@link ta.nemahuta.neo4j.structure.Neo4JEdge}s
     */
    private final Function<Driver, Neo4JElementIdAdapter<?>> edgeIdAdapterFactory;

    /**
     * the host name to be used for connection
     */
    @NonNull
    @Getter(onMethod = @__(@Nonnull))
    private final String hostname;

    /**
     * the port to be used for connection
     */
    @Getter
    private short port;

    /**
     * the authentication token to be used
     */
    @NonNull
    @Getter(onMethod = @__(@Nonnull))
    private final AuthToken authToken;

    /**
     * the optional graph name to be used to partition the graph
     */
    @Getter(onMethod = @__(@Nullable))
    private final String graphName;

    @Getter(onMethod = @__(@Nullable))
    private final Consumer<Config.ConfigBuilder> additionConfiguration;

    @Getter
    private final boolean profilingEnabled;

    /**
     * Creates the {@link Neo4JElementIdAdapter} for the {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es.
     *
     * @param driver the driver to use for creation
     * @return the adapter
     */
    @Nonnull
    public Neo4JElementIdAdapter<?> createVertexIdAdapter(@Nonnull @NonNull final Driver driver) {
        return Optional.ofNullable(vertexIdAdapterFactory).orElse(DEFAULT_ID_ADAPTER_FACTORY).apply(driver);
    }

    /**
     * Creates the {@link Neo4JElementIdAdapter} for the {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es.
     *
     * @param driver the driver to use for creation
     * @return the adapter
     */
    @Nonnull
    public Neo4JElementIdAdapter<?> createEdgeIdAdapter(@Nonnull @NonNull final Driver driver) {
        return Optional.ofNullable(edgeIdAdapterFactory).orElse(DEFAULT_ID_ADAPTER_FACTORY).apply(driver);
    }

    @Nonnull
    public Configuration toApacheConfiguration() {
        final Configuration result = new BaseConfiguration();
        // TODO add stuff here
        return result;
    }

}
