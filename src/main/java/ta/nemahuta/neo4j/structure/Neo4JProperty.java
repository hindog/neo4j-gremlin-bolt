package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;

import static ta.nemahuta.neo4j.property.AbstractPropertyFactory.getCardinalityAndIterable;

/**
 * Wrapper for a {@link Property} in Neo4j.
 *
 * @param <P> the type of the parent element in Neo4J
 * @param <T> the type of the property value
 */
public abstract class Neo4JProperty<P extends Neo4JElement, T> implements Property<T> {

    @NonNull
    protected final P parent;

    @NonNull
    protected final String key;

    @NonNull
    private final Iterable<T> wrapped;

    @NonNull
    @Getter(onMethod = @__(@Nonnull))
    private final VertexProperty.Cardinality cardinality;

    public Neo4JProperty(@Nonnull @NonNull final P parent,
                         @Nonnull @NonNull final String key,
                         @Nonnull @NonNull final Iterable<T> wrapped,
                         @Nonnull @NonNull final VertexProperty.Cardinality cardinality) {
        this.parent = parent;
        this.key = key;
        this.wrapped = wrapped;
        if (!wrapped.iterator().hasNext() && VertexProperty.Cardinality.single.equals(cardinality)) {
            throw new IllegalArgumentException("No value specified.");
        }
        this.cardinality = cardinality;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public T value() throws NoSuchElementException {
        return (T) getValue();
    }

    /**
     * @return the wrapped value as an object depending on its cardinality
     */
    protected Object getValue() {
        switch (cardinality) {
            case set:
                return ImmutableSet.copyOf(wrapped);
            case list:
                return ImmutableList.copyOf(wrapped);
            case single:
                return wrapped.iterator().next();
            default:
                throw new IllegalArgumentException("Could not determine how to handle property cardinality: " + cardinality);
        }
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public P element() {
        return parent;
    }

    @Override
    public void remove() {
        // Remove the property by changing the parent's state
        parent.getState().modify(s -> s.withProperties(ImmutableMap.<String, Neo4JProperty<? extends Neo4JElement, ?>>builder().putAll(Maps.filterKeys(s.properties, k -> !this.key.equals(k))).build()));
    }

    /**
     * Create a new property with the provided value.
     *
     * @param value the value to be used
     * @param <V>   the type of the value
     * @return the property
     */
    @Nonnull
    public <V> Neo4JProperty<P, ?> withValue(@Nonnull @NonNull final V value) {
        final Pair<VertexProperty.Cardinality, Iterable<?>> parms = getCardinalityAndIterable(value);
        return withValue(parms.getValue0(), parms.getValue1());
    }

    @Nonnull
    protected abstract Neo4JProperty<P, ?> withValue(@Nonnull final VertexProperty.Cardinality cardinality,
                                                     @Nonnull final Iterable<?> value);

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Property && ElementHelper.areEqual(this, object);
    }

}
