package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;
import ta.nemahuta.neo4j.id.Neo4JTransientElementIdGenerator;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JVertex;
import ta.nemahuta.neo4j.structure.Neo4JVertexProperty;

import javax.annotation.Nonnull;

public class Neo4JVertexPropertyFactory extends AbstractPropertyFactory<Neo4JVertexProperty<?>> {

    private final Neo4JElementIdGenerator<?> idGenerator;

    public Neo4JVertexPropertyFactory(@Nonnull @NonNull final Neo4JElementIdAdapter<?> idAdapter) {
        super(ImmutableSet.of(idAdapter.propertyName()));
        this.idGenerator = new Neo4JTransientElementIdGenerator();
    }

    @Nonnull
    @Override
    protected Neo4JVertexProperty<?> create(@NonNull @Nonnull final Neo4JElement parent,
                                            @NonNull @Nonnull final String key,
                                            @NonNull @Nonnull final Iterable<?> wrapped,
                                            @NonNull @Nonnull final VertexProperty.Cardinality cardinality) {
        return new Neo4JVertexProperty<>((Neo4JVertex) parent, idGenerator.generate(), key, wrapped, cardinality);
    }
}
