package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;
import ta.nemahuta.neo4j.state.LocalAndRemoteStateHolder;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Abstract implementation of an {@link Element} for Neo4J.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@ToString
public abstract class Neo4JElement implements Element {

    @Getter
    private final LocalAndRemoteStateHolder<Neo4JElementState> state;

    protected final Neo4JGraph graph;
    protected final AbstractPropertyFactory<? extends Neo4JProperty<? extends Neo4JElement, ?>> propertyFactory;

    protected Neo4JElement(@Nonnull @NonNull final Neo4JGraph graph,
                           @Nonnull @NonNull final Neo4JElementId<?> id,
                           @Nonnull @NonNull final ImmutableSet<String> labels,
                           @Nonnull @NonNull final Optional<MapAccessor> propertyAccessor,
                           @Nonnull @NonNull final AbstractPropertyFactory<? extends Neo4JProperty<? extends Neo4JElement, ?>> propertyFactory) {
        this.graph = graph;
        this.propertyFactory = propertyFactory;
        // The initial sync state is synchronous if the property accessor was provided, otherwise this is a transient element
        final SyncState initialSyncState = propertyAccessor.map(p -> SyncState.SYNCHRONOUS).orElse(SyncState.TRANSIENT);
        // In case a property accessor is provided, we create the properties, otherwise we use an empty properties map
        final ImmutableMap<String, ? extends Neo4JProperty<? extends Neo4JElement, ?>> properties = propertyAccessor
                .map(p -> propertyFactory.create(this, p))
                .orElse(ImmutableMap.of());
        final StateHolder<Neo4JElementState> stateHolder = new StateHolder<>(initialSyncState, new Neo4JElementState(id, labels, properties));
        this.state = new LocalAndRemoteStateHolder<>(stateHolder);
    }


    @Override
    public Neo4JElementId<?> id() {
        return state.current(s -> s.id);
    }

    @Override
    public String label() {
        return state.current(s -> String.join("::", s.labels));
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        state.delete();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    protected <V, P extends Property<V>> Iterator<P> properties(@Nonnull @NonNull final Supplier<P> emptySupplier,
                                                                @Nonnull @NonNull final String... propertyKeys) {
        return this.getState().current(s ->
                Stream.of(propertyKeys).map(k ->
                        Optional.ofNullable((P) s.properties.get(k))
                                .orElseGet(emptySupplier))
        ).iterator();
    }

    protected <V, P extends Property<V>> P property(@Nonnull @NonNull final String key,
                                                    @Nullable final V value,
                                                    @Nonnull @NonNull Supplier<P> emptySupplier) {
        ElementHelper.validateProperty(key, value);
        if (value == null) {
            properties(key).next().remove();
            return emptySupplier.get();
        }
        final Object[] result = new Object[1];
        this.getState().modify(s -> {
            final Neo4JProperty<? extends Neo4JElement, ?> property = s.properties.get(key);
            final Neo4JProperty<? extends Neo4JElement, ?> modifiedProperty = property != null ? property.withValue(value) : propertyFactory.create(this, key, value);
            result[0] = modifiedProperty;
            return s.withProperties(ImmutableMap.<String, Neo4JProperty<? extends Neo4JElement, ?>>builder()
                    .putAll(s.properties).put(key, modifiedProperty).build());
        });
        return (P) result[0];
    }

}
