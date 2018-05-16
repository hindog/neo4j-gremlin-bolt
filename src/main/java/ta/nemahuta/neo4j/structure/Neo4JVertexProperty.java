package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import ta.nemahuta.neo4j.id.Neo4JElementId;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class Neo4JVertexProperty<T> extends Neo4JProperty<Neo4JVertex, T> implements VertexProperty<T> {

    private final Neo4JElementId<?> id;

    public Neo4JVertexProperty(@NonNull @Nonnull final Neo4JVertex parent,
                               @NonNull @Nonnull final Neo4JElementId<?> id,
                               @NonNull @Nonnull final String key,
                               @NonNull @Nonnull final Iterable<T> wrapped,
                               @NonNull @Nonnull final Cardinality cardinality) {
        super(parent, key, wrapped, cardinality);
        this.id = id;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Nonnull
    @Override
    protected Neo4JProperty<Neo4JVertex, ?> withValue(@Nonnull @NonNull final Cardinality cardinality,
                                                      @Nonnull @NonNull final Iterable<?> value) {
        return new Neo4JVertexProperty<>(parent, id, key, value, cardinality);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof VertexProperty && ElementHelper.areEqual(this, object);
    }


}
