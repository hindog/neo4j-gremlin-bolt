package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VertexEdgeReferencesTest {

    private VertexEdgeReferences sut = new VertexEdgeReferences();

    @Test
    void withNewEdgelabelIsNotKnownAddEdge() {
        // when: 'adding an edge for a label which is not known, while not all labels are known'
        sut = sut.withNewEdge("x", 1l);
        // then: 'the edge id will not be returned, as not all of the ids are known for the label'
        assertEquals(null, sut.get("x"));
    }

    @Test
    void withNewEdgelabelIsKnownAddEdge() {
        // when: 'the label is known and has an edge'
        sut = sut.withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(2l)));
        // and: 'a new edge is registered'
        sut = sut.withNewEdge("x", 1l);
        // then: 'the union of those known edge ids is returned'
        assertEquals(ImmutableSet.of(1l, 2l), sut.get("x"));
    }

    @Test
    void withNewEdgeallLabelKnownAddEdge() {
        // when: 'all edges are known, but the label has no edge id yet'
        sut = sut.withAllResolvedEdges(ImmutableMap.of("y", ImmutableSet.of(2l)));
        // and: 'a new edge is registered for which no entry existed'
        sut = sut.withNewEdge("x", 1l);
        // then: 'the new edge is returned'
        assertEquals(ImmutableSet.of(1l), sut.get("x"));
    }

    @Test
    void withNewEdgeWhichIsKnownAlready() {
        // when: 'the edge is known already'
        sut = sut.withAllResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l)));
        // and: 'registered again'
        final VertexEdgeReferences newSut = sut.withNewEdge("x", 1l);
        assertTrue(newSut == sut);
    }

    @Test
    void withAllResolvedEdges() {
        // when: 'resolving all edges'
        sut = sut.withAllResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l), "y", ImmutableSet.of(2l, 3l)));
        // then: 'all labels are known'
        assertEquals(ImmutableSet.of("x", "y"), sut.getLabels());
        // and: 'all edges are provided'
        assertEquals(ImmutableSet.of(1l, 2l, 3l), ImmutableSet.copyOf(sut.getAllKnown().iterator()));
    }

    @Test
    void withPartialResolvedEdges() {
        // when: 'resolving all edges'
        sut = sut.withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l), "y", ImmutableSet.of(2l, 3l)));
        // then: 'the labels are not known'
        assertEquals(null, sut.getLabels());
        // and: 'all edges are provided'
        assertEquals(ImmutableSet.of(1l, 2l, 3l), ImmutableSet.copyOf(sut.getAllKnown().iterator()));
    }

    @Test
    void withRemovedEdgesEdgeUnknown() {
        // when: 'the edges to be removed are not known'
        sut = sut.withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l), "y", ImmutableSet.of(2l, 3l)));
        // and: 'edges are removed'
        final VertexEdgeReferences newSut = sut.withRemovedEdges(ImmutableSet.of(4l, 5l, 6l));
        // then: 'the same instance is returned'
        assertTrue(sut == newSut);
    }

    @Test
    void withRemovedEdgesEdgeSomeKnown() {
        // when: 'some of the edges to be removed are not known'
        sut = sut.withPartialResolvedEdges(ImmutableMap.of("x", ImmutableSet.of(1l), "y", ImmutableSet.of(2l, 3l)));
        // and: 'edges are removed'
        final VertexEdgeReferences newSut = sut.withRemovedEdges(ImmutableSet.of(1l, 3l, 4l, 5l, 6l));
        // then: 'another instance is returned'
        assertTrue(sut != newSut);
        // and: 'the remaining edges are returned'
        assertEquals(ImmutableSet.of(2l), ImmutableSet.copyOf(newSut.getAllKnown().iterator()));
        // and: ' the edges for the label do not contain the removed edge'
        assertEquals(ImmutableSet.of(2l), newSut.get("y"));
        // and: ' the edges are empty for labels which contained the removed edge only'
        assertEquals(ImmutableSet.of(), newSut.get("x"));
    }


}