package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import javax.annotation.Nonnull;

public class Neo4JEdgeProperty<T> extends Neo4JProperty<Neo4JEdge, T> {

    public Neo4JEdgeProperty(@NonNull @Nonnull final Neo4JEdge parent,
                             @NonNull @Nonnull final String key,
                             @NonNull @Nonnull final Iterable<T> wrapped,
                             @NonNull @Nonnull final VertexProperty.Cardinality cardinality) {
        super(parent, key, wrapped, cardinality);
    }

    @Nonnull
    @Override
    protected Neo4JProperty<Neo4JEdge, ?> withValue(@Nonnull @NonNull final VertexProperty.Cardinality cardinality,
                                                    @Nonnull @NonNull final Iterable<?> value) {
        return new Neo4JEdgeProperty<>(parent, key, value, cardinality);
    }

}
