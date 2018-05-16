package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
public abstract class AbstractPropertyFactory<T extends Neo4JProperty<? extends Neo4JElement, ?>> {

    protected final ImmutableSet<String> excludedKeys;

    /**
     * Check if the key of the property is excluded.
     *
     * @param key the key to be checked
     * @return {@code true} if the key is excluded, {@code false} otherwise
     */
    protected boolean isExcluded(@Nonnull String key) {
        return excludedKeys.contains(key);
    }

    /**
     * Create a new property from a value.
     *
     * @param parent      the parent element
     * @param key         the key of the property
     * @param wrapped     the value to be wrapped (as an iterable)
     * @param cardinality the cardinality to be used
     * @return the new property
     */
    @Nonnull
    protected abstract T create(@Nonnull @NonNull final Neo4JElement parent,
                                @Nonnull @NonNull final String key,
                                @Nonnull @NonNull final Iterable<?> wrapped,
                                @Nonnull @NonNull final VertexProperty.Cardinality cardinality);

    /**
     * Create an entirely new property.
     *
     * @param parent  the parent
     * @param key     the key
     * @param wrapped the value to be wrapped
     * @return the new property
     */
    public T create(@Nonnull @NonNull final Neo4JElement parent,
                    @Nonnull @NonNull final String key,
                    @Nonnull @NonNull final Object wrapped) {
        final Pair<VertexProperty.Cardinality, Iterable<?>> parms = getCardinalityAndIterable(wrapped);
        return create(parent, key, parms.getValue1(), parms.getValue0());
    }

    /**
     * Create a new property {@link ImmutableMap} for a {@link MapAccessor} of a driver's node.
     *
     * @param parent the parent element to create the properties for
     * @param node   the node to be used to extract the properties
     * @return the map containing the properties
     */
    @Nonnull
    public ImmutableMap<String, T> create(@Nonnull @NonNull final Neo4JElement parent,
                                          @Nonnull @NonNull final MapAccessor node) {
        final ImmutableMap.Builder<String, T> builder = ImmutableMap.builder();
        for (final String key : node.keys()) {
            if (!isExcluded(key)) {
                Optional.ofNullable(create(parent, key, node.get(key))).ifPresent(v -> builder.put(key, v));
            }
        }
        return builder.build();
    }

    /**
     * Create a new {@link Neo4JProperty} for a single value.
     *
     * @param parent the parent of the property
     * @param key    the key for the property
     * @param value  the driver's value to extract
     * @return the new properties
     */
    @Nullable
    public T create(@Nonnull @NonNull final Neo4JElement parent,
                    @Nonnull @NonNull final String key,
                    @Nonnull @NonNull final Value value) {
        return Optional.ofNullable(readUsingType(value))
                .map(v -> create(parent, key, v))
                .orElse(null);
    }

    @Nullable
    protected Object readUsingType(@Nonnull @NonNull final Value value) {
        final TypeRepresentation type = (TypeRepresentation) value.type();
        final TypeConstructor typeConstructor = type.constructor();
        switch (Optional.ofNullable(typeConstructor)
                .orElseThrow(() -> new IllegalArgumentException("Encountered a null typed property"))) {
            case LIST_TyCon:
                return value.asList();
            case BOOLEAN_TyCon:
                return value.asBoolean();
            case BYTES_TyCon:
                return value.asByteArray();
            case FLOAT_TyCon:
                return value.asFloat();
            case INTEGER_TyCon:
                return value.asObject();
            case NUMBER_TyCon:
                return value.asNumber();
            case STRING_TyCon:
                return value.asString();
            case NULL_TyCon:
                return null;
            default:
                throw new IllegalArgumentException("Determined unhandled type: " + typeConstructor.typeName());
        }
    }

    public static Pair<VertexProperty.Cardinality, Iterable<?>> getCardinalityAndIterable(@Nonnull @NonNull final Object value) {
        if (value instanceof Set) {
            return new Pair<>(VertexProperty.Cardinality.set, ImmutableSet.copyOf((Set) value));
        } else if (value instanceof Iterable) {
            return new Pair<>(VertexProperty.Cardinality.list, ImmutableList.copyOf((Iterable) value));
        } else if (value.getClass().isArray()) {
            return new Pair<>(VertexProperty.Cardinality.list, ImmutableList.copyOf((Object[]) value));
        } else {
            return new Pair<>(VertexProperty.Cardinality.single, ImmutableSet.of(value));
        }
    }

}
