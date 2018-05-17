package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LazyEdgeProviderTest {

    @Mock
    private Neo4JVertex vertex;

    @Mock
    private Function<Iterable<String>, Stream<Neo4JEdge>> edgeRetriever;

    @Mock
    private Neo4JEdge edgeA1, edgeB1, edgeB2;
    private LazyEdgeProvider sut;
    private final ArgumentMatcher<Iterable<String>> aLabelMatcher = a -> a != null && ImmutableSet.copyOf(a).equals(ImmutableSet.of("A"));
    private final ArgumentMatcher<Iterable<String>> bLabelMatcher = a -> a != null && ImmutableSet.copyOf(a).equals(ImmutableSet.of("B"));

    @BeforeEach
    void stubAndCreateSut() {
        when(edgeA1.label()).thenReturn("A");
        when(edgeB1.label()).thenReturn("B");
        when(edgeB2.label()).thenReturn("B");
        this.sut = new LazyEdgeProvider(vertex, edgeRetriever, false);

        when(edgeRetriever.apply(argThat(aLabelMatcher))).thenReturn(Stream.of(edgeA1));
        when(edgeRetriever.apply(argThat(bLabelMatcher))).thenReturn(Stream.of(edgeB1));
        when(edgeRetriever.apply(eq(ImmutableSet.of()))).thenReturn(Stream.of(edgeA1, edgeB1));
    }

    @Test
    void provideEdges() {
        // when: 'requesting an edge for which the label is not retrieved'
        final Iterable<Neo4JEdge> actualEdgeAs = sut.provideEdges("A");
        // then: 'the single A1 edge is returned'
        assertEdges(actualEdgeAs, edgeA1);
        verify(edgeRetriever, times(1)).apply(argThat(aLabelMatcher));
        // when: 'requesting the edges again'
        sut.provideEdges("A");
        // then: 'the edges are cached'
        verify(edgeRetriever, times(1)).apply(argThat(aLabelMatcher));

        // when: 'requesting all edges'
        final Iterable<Neo4JEdge> actualEdgeAll = sut.provideEdges();
        // then: 'the all edges are returned'
        assertEdges(actualEdgeAll, edgeA1, edgeB1);
    }

    @Test
    void registerEdge() {
        // setup: 'a cached edge'
        sut.registerEdge(edgeB2);
        // when: 'requesting an edge for which the label is not retrieved'
        final Iterable<Neo4JEdge> actualEdgeAs = sut.provideEdges("B");
        // then: 'the single A1 edge is returned'
        assertEdges(actualEdgeAs, edgeB1, edgeB2);
        verify(edgeRetriever, times(1)).apply(argThat(bLabelMatcher));
    }

    @Test
    void completeLyLoaded() {
        // setup: 'a completely loaded provider'
        this.sut = new LazyEdgeProvider(vertex, edgeRetriever, true);
        // when: 'requesting an edge for which the label is not retrieved'
        final Iterable<Neo4JEdge> actualEdgeAs = sut.provideEdges("B");
        // then: 'no edges are returned'
        assertEdges(actualEdgeAs);
        // when: 'adding an edge'
        sut.registerEdge(edgeB2);
        // then: 'the edge is returned for its label'
        assertEdges(sut.provideEdges("B"), edgeB2);
        // and: 'no interactions with the retriever'
        verify(edgeRetriever, never()).apply(any());

    }

    private void assertEdges(final Iterable<Neo4JEdge> actual, final Neo4JEdge... expected) {
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(actual));
    }


}