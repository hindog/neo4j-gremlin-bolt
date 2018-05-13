package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Property;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;
import ta.nemahuta.neo4j.state.PropertyCardinality;
import ta.nemahuta.neo4j.state.PropertyValue;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Wrapper for a {@link Property} in Neo4j.
 *
 * @param <P> the type of the parent element in Neo4J
 * @param <T> the type of the property value
 */
public class Neo4JProperty<P extends Neo4JElement, T> implements Property<T> {

    protected final P parent;
    protected final String key;

    public Neo4JProperty(final P parent, final String key) {
        this.parent = Objects.requireNonNull(parent, "parent may not be null");
        this.key = Objects.requireNonNull(key, "key may not be null");
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public T value() throws NoSuchElementException {
        return (T) parent.currentState(s -> Optional.ofNullable(s.properties.get(key)).flatMap(PropertyValue::asOptional).get());
    }

    /**
     * Set a value.
     *
     * @param idGenerator the identifier generator for the property (to be used, if it does not exist)
     * @param cardinality the cardinality to be used for the property
     * @param value       the value to be set
     */
    public void setValue(@Nonnull @NonNull final Neo4JElementIdGenerator<?> idGenerator,
                         @Nonnull @NonNull final PropertyCardinality cardinality,
                         @Nonnull @NonNull final T value) {
        parent.state.modify(s -> {
            final PropertyValue<?> propertyValue = Optional.ofNullable(s.properties.get(key))
                    .map(e -> ((PropertyValue<T>) e).with(cardinality, value))
                    .orElseGet(() -> PropertyValue.from(idGenerator.generate(), value, cardinality));
            return s.withProperties(ImmutableMap.<String, PropertyValue<?>>builder().putAll(s.properties).put(key, propertyValue).build());
        });
    }

    @Override
    public boolean isPresent() {
        return parent.currentState(s -> Optional.ofNullable(s.properties.get(key)).flatMap(PropertyValue::asOptional).isPresent());
    }

    @Override
    public P element() {
        return parent;
    }

    @Override
    public void remove() {
        parent.modify(s -> s.withProperties(ImmutableMap.<String, PropertyValue<?>>builder().putAll(Maps.filterKeys(s.properties, k -> !this.key.equals(k))).build()));
    }

}
