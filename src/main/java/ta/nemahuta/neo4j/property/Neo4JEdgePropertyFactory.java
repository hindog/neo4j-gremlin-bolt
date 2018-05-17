package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JEdgeProperty;
import ta.nemahuta.neo4j.structure.Neo4JElement;

import javax.annotation.Nonnull;

public class Neo4JEdgePropertyFactory extends AbstractPropertyFactory<Neo4JEdgeProperty<?>> {

    public Neo4JEdgePropertyFactory(@Nonnull @NonNull final Neo4JElementIdAdapter<?> adapter) {
        super(adapter.propertyName().map(ImmutableSet::of).orElse(ImmutableSet.of()));
    }

    @Nonnull
    @Override
    protected Neo4JEdgeProperty<?> create(@NonNull @Nonnull final Neo4JElement parent,
                                          @NonNull @Nonnull final String key,
                                          @NonNull @Nonnull final Iterable<?> wrapped,
                                          @NonNull @Nonnull final VertexProperty.Cardinality cardinality) {
        return new Neo4JEdgeProperty<>((Neo4JEdge) parent, key, wrapped, cardinality);
    }
}
