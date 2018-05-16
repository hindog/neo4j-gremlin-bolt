package ta.nemahuta.neo4j.query;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;
import ta.nemahuta.neo4j.structure.Neo4JElement;

import javax.annotation.Nonnull;

public class TestNeo4JPropertyFactory extends AbstractPropertyFactory<TestNeo4JProperty<?>> {

    public TestNeo4JPropertyFactory() {
        super(ImmutableSet.of("id"));
    }

    @Nonnull
    @Override
    protected TestNeo4JProperty<?> create(@Nonnull final Neo4JElement parent, @Nonnull final String key, @Nonnull final Iterable<?> wrapped, @Nonnull final VertexProperty.Cardinality cardinality) {
        return new TestNeo4JProperty<>(parent, key, wrapped, cardinality);
    }
}
