package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Neo4JEdgeStateTest {

    private final String label = "A";
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("X", "Y");
    private final long inId = 1l, outId = 2l;

    private final Neo4JEdgeState sut = new Neo4JEdgeState(label, properties, inId, outId);

    @Test
    void withProperties() {
        final ImmutableMap<String, Object> newProps = ImmutableMap.of("A", "B");
        assertEquals(newProps, sut.withProperties(newProps).getProperties());
    }

    @Test
    void initialValues() {
        assertEquals(label, sut.getLabel());
        assertEquals(properties, sut.getProperties());
        assertEquals(inId, sut.getInVertexId());
        assertEquals(outId, sut.getOutVertexId());
    }

    @Test
    void equals() {
        assertEquals(sut, sut.withProperties(properties));
        assertNotEquals(sut, sut.withProperties(ImmutableMap.of()));
    }

    @Test
    void checkHashCode() {
        assertEquals(sut.hashCode(), sut.withProperties(properties).hashCode());
    }

    @Test
    void checkToString() {
        assertEquals(sut.toString(), sut.withProperties(properties).toString());
    }
}