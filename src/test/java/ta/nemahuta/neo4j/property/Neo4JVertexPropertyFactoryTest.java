package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.structure.Neo4JVertex;
import ta.nemahuta.neo4j.structure.Neo4JVertexProperty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class Neo4JVertexPropertyFactoryTest {
    @Test
    void create() {
        final Neo4JVertex vertex = mock(Neo4JVertex.class);

        final Neo4JVertexPropertyFactory sut = new Neo4JVertexPropertyFactory(new Neo4JNativeElementIdAdapter());
        final Neo4JVertexProperty<?> prop = sut.create(vertex, "x", ImmutableSet.of("x"), VertexProperty.Cardinality.set);
        assertNotNull(prop);
        assertEquals(new Neo4JTransientElementId<>(1l), prop.id());
    }
}