package ta.nemahuta.neo4j.config;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A configuration for {@link ta.nemahuta.neo4j.structure.Neo4JGraph}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Builder
@ToString
@EqualsAndHashCode
@Slf4j
public class Neo4JConfiguration {

    private static final Map<Class<?>, Class<?>> BOXING_DEFINITION = ImmutableMap.<Class<?>, Class<?>>builder()
            .put(boolean.class, Boolean.class)
            .put(byte.class, Byte.class)
            .put(char.class, Character.class)
            .put(short.class, Short.class)
            .put(double.class, double.class)
            .put(float.class, float.class)
            .put(int.class, Integer.class)
            .put(long.class, Long.class)
            .build();

    public static final Function<Driver, Neo4JElementIdAdapter<?>> DEFAULT_ID_ADAPTER_FACTORY = driver -> new Neo4JNativeElementIdAdapter();

    /**
     * the factory for the {@link Neo4JElementIdAdapter} for {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es
     */
    @ConfigurationKey
    private final Function<Driver, Neo4JElementIdAdapter<?>> vertexIdAdapterFactory;

    /**
     * the factory for the {@link Neo4JElementIdAdapter} for {@link ta.nemahuta.neo4j.structure.Neo4JEdge}s
     */
    @ConfigurationKey
    private final Function<Driver, Neo4JElementIdAdapter<?>> edgeIdAdapterFactory;

    /**
     * the host name to be used for connection
     */
    @NonNull
    @Getter(onMethod = @__(@Nonnull))
    @ConfigurationKey
    private final String hostname;

    /**
     * the port to be used for connection
     */
    @Getter
    @ConfigurationKey
    private int port = 27017;

    /**
     * the authentication token to be used
     */
    @NonNull
    @Getter(onMethod = @__(@Nonnull))
    @ConfigurationKey
    private final AuthToken authToken;

    /**
     * the optional graph name to be used to partition the graph
     */
    @Getter(onMethod = @__(@Nullable))
    @ConfigurationKey
    private final String graphName;

    @Getter(onMethod = @__(@Nullable))
    @ConfigurationKey
    private final Consumer<Config.ConfigBuilder> additionConfiguration;

    @Getter
    @ConfigurationKey
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
        configurationFields()
                .forEach(f -> {
                    try {
                        final String key = !StringUtils.isEmpty(f.getAnnotation(ConfigurationKey.class).value()) ?
                                f.getAnnotation(ConfigurationKey.class).value() :
                                f.getName();
                        result.addProperty(key, f.get(Neo4JConfiguration.this));
                    } catch (final IllegalAccessException e) {
                        throw new IllegalStateException("Could not read configuration field", e);
                    }
                });
        return result;
    }

    @Nonnull
    private static Stream<Field> configurationFields() {
        return Stream.of(Neo4JConfiguration.class.getDeclaredFields())
                .filter(f -> Objects.nonNull(f.getAnnotation(ConfigurationKey.class)));
    }

    @Nonnull
    public static Neo4JConfiguration fromApacheConfiguration(@Nonnull @NonNull final Configuration configuration) {
        final Neo4JConfigurationBuilder builder = builder();
        configurationFields().forEach(f -> {
            final Object value = configuration.getProperty(f.getName());
            if (value == null) {
                return;
            }
            try {
                final Method builderMethod = findBuilderMethod(f.getName(), value.getClass());
                builderMethod.invoke(builder, value);
            } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                log.warn("Could not set value " + value + " on builder for field " + f.getName(), ex);
            }
        });
        return builder.build();
    }

    @Nonnull
    private static Method findBuilderMethod(@Nonnull @NonNull final String name,
                                            @Nonnull @NonNull final Class<?> paramClass) throws NoSuchMethodException {

        return Stream.of(Neo4JConfigurationBuilder.class.getDeclaredMethods())
                .filter(m -> name.equals(m.getName()))
                .filter(m -> m.getParameterTypes().length == 1 && canCoerce(paramClass, m.getParameterTypes()[0]))
                .findAny()
                .orElseThrow(() -> new NoSuchMethodException("Could not find method '" + name + "' which accepts " + paramClass.getName()));
    }

    private static boolean canCoerce(@Nonnull @NonNull final Class<?> fromClass,
                                     @Nonnull @NonNull final Class<?> toClass) {
        return BOXING_DEFINITION.getOrDefault(toClass, toClass).isAssignableFrom(BOXING_DEFINITION.getOrDefault(fromClass, fromClass));
    }

}
