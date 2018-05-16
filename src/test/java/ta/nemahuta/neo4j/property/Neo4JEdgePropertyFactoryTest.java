package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JEdgeProperty;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class Neo4JEdgePropertyFactoryTest {

    @Test
    void create() {
        final Neo4JEdge edge = mock(Neo4JEdge.class);

        final Neo4JEdgePropertyFactory sut = new Neo4JEdgePropertyFactory(new Neo4JNativeElementIdAdapter());
        final Neo4JEdgeProperty<?> prop = sut.create(edge, "x", ImmutableSet.of("x"), VertexProperty.Cardinality.set);

        assertNotNull(prop);
    }

}