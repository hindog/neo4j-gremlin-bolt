package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Neo4JVertexStateTest {

    private final ImmutableSet<String> labels = ImmutableSet.of("A", "B");
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("X", "Y");

    private final Neo4JVertexState sut = new Neo4JVertexState(labels, properties);

    @Test
    void withProperties() {
        final ImmutableMap<String, Object> newProps = ImmutableMap.of("A", "B");
        assertEquals(newProps, sut.withProperties(newProps).getProperties());
    }

    @Test
    void initialValues() {
        assertEquals(labels, sut.getLabels());
        assertEquals(properties, sut.getProperties());
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