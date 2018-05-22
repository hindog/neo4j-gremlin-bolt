package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Abstract implementation of an {@link Element} for Neo4J.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@ToString
@RequiredArgsConstructor
public abstract class Neo4JElement<S extends Neo4JElementState, P extends Property>
        implements Element {

    protected final Neo4JGraph graph;
    protected final long id;
    protected final Neo4JElementStateScope<S> scope;

    private final Map<String, P> propertyMap = new ConcurrentHashMap<>();

    @Override
    @Nonnull
    public Long id() {
        return id;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    protected P getProperty(@Nonnull @NonNull final String key, final Object value) {
        ElementHelper.validateProperty(key, value);
        updateState(state -> computePropertyChangedState(key, value, state));
        return getProperties(key).findAny().get();
    }

    protected Stream<P> getProperties(@Nonnull @NonNull final String... propertyKeys) {
        return (propertyKeys.length == 0 ? getState().getProperties().keySet().stream() : Stream.of(propertyKeys))
                .map(this::getOrCreateProperty)
                .filter(Objects::nonNull);
    }

    @Nullable
    private P getOrCreateProperty(@Nonnull final String key) {
        final Object value = getState().getProperties().get(key);
        if (value == null) {
            propertyMap.remove(key);
            return createEmptyProperty();
        }
        return Optional.ofNullable(propertyMap.get(key))
                .orElseGet(() -> {
                    final P result = createNewProperty(key);
                    return result;
                });
    }

    /**
     * Create a new property for the provided key.
     *
     * @param key the key of the property
     * @return the property
     */
    protected abstract P createNewProperty(final String key);

    /**
     * @return an empty property
     */
    protected abstract P createEmptyProperty();

    private S computePropertyChangedState(final @Nonnull @NonNull String key, final Object value, final S state) {
        // Build a new map with the propertyMap except the one being set
        final ImmutableMap.Builder<String, Object> builder =
                ImmutableMap.<String, Object>builder().putAll(Maps.filterKeys(state.getProperties(), k -> !Objects.equals(k, key)));
        if (value != null) {
            // Only set the new property, if it is not null
            builder.put(key, value);
        } else {
            propertyMap.remove(key);
        }
        return (S) state.withProperties(builder.build());
    }

    @Override
    public void remove() {
        scope.delete(id);
    }


    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    /**
     * @return the state of the element
     */
    protected S getState() {
        return Optional.of(scope.get(id)).orElseThrow(() -> new IllegalStateException("Element has been deleted in the scope: " + id));
    }

    protected void updateState(@Nonnull @NonNull final Function<S, S> stateUpdate) {
        scope.modify(id, stateUpdate);
    }

}
