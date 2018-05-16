package ta.nemahuta.neo4j.query;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import javax.annotation.Nonnull;

public class TestNeo4JProperty<V> extends Neo4JProperty<Neo4JElement, V> {

    public TestNeo4JProperty(@Nonnull final Neo4JElement parent, @Nonnull final String key, @Nonnull final Iterable<V> wrapped, @Nonnull final VertexProperty.Cardinality cardinality) {
        super(parent, key, wrapped, cardinality);
    }

    @Nonnull
    @Override
    protected Neo4JProperty<Neo4JElement, ?> withValue(@Nonnull final VertexProperty.Cardinality cardinality, @Nonnull final Iterable<?> value) {
        return new TestNeo4JProperty<>(parent, key, value, cardinality);
    }
}
