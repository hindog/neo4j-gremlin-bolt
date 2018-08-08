package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Neo4JVertexStateTest {

    private final ImmutableSet<String> labels = ImmutableSet.of("A", "B");
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("X", "Y");

    private Neo4JVertexState sut = new Neo4JVertexState(labels, properties);

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
    
    @Test
    void withRemovedEdgesReturnsThisWithNoChange() {
        sut = sut.withEdgeIds(Direction.IN, sut.getEdgeIds(Direction.IN).withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l))))
                .withEdgeIds(Direction.OUT, sut.getEdgeIds(Direction.OUT).withPartialResolvedEdges(ImmutableMap.of("y", ImmutableSet.of(2l))));
        // when: 'removing edges which do not exist'
        final Neo4JVertexState newSut = sut.withRemovedEdges(ImmutableSet.of(3l));
        // then: 'the instance stays the same'
        assertTrue(newSut == sut);
    }

    @Test
    void withRemovedEdgesReturnsOtherWithChange() {
        sut = sut.withEdgeIds(Direction.IN, sut.getEdgeIds(Direction.IN).withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l))))
                .withEdgeIds(Direction.OUT, sut.getEdgeIds(Direction.OUT).withPartialResolvedEdges(ImmutableMap.of("y", ImmutableSet.of(2l))));
        // when: 'removing edges which does exist in IN'
        final Neo4JVertexState newInSut = sut.withRemovedEdges(ImmutableSet.of(1l));
        // then: 'the instance stays the same'
        assertTrue(newInSut != sut);
        // when: 'removing edges which does exist in OUT'
        final Neo4JVertexState newOutSut = newInSut.withRemovedEdges(ImmutableSet.of(2l));
        // then: 'the instance stays the same'
        assertTrue(newOutSut != newInSut);
    }

}