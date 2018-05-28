package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;

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

    public Neo4JProperty(@Nonnull final P parent,
                         @Nonnull final String key) {
        this.parent = parent;
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public T value() throws NoSuchElementException {
        return (T) parent.getState().getProperties().get(key);
    }

    @Override
    public boolean isPresent() {
        return parent.getState().getProperties().containsKey(key);
    }

    @Override
    public P element() {
        return parent;
    }

    @Override
    public void remove() {
        parent.property(key, null);
    }

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
