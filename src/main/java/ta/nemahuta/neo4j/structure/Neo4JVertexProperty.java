package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * {@link VertexProperty} implementation for neo4j.
 *
 * @param <T> the type of the property
 */
public class Neo4JVertexProperty<T> extends Neo4JProperty<Neo4JVertex, T> implements VertexProperty<T> {

    private final String id;

    public Neo4JVertexProperty(@Nonnull final Neo4JVertex parent,
                               @Nonnull final String key) {
        super(parent, key);
        this.id = parent.id() + "." + key;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        assertPresent();
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    @Nonnull
    public Object id() {
        assertPresent();
        return id;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        assertPresent();
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof VertexProperty && ElementHelper.areEqual(this, object);
    }


}
