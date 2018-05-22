package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LazyEdgeProviderTest {

    @Mock
    private Neo4JVertex vertex;

    @Mock
    private Function<Set<String>, Map<String, Set<Long>>> edgeRetriever;

    private final long edgeA1 = 1l, edgeB1 = 2l, edgeB2 = 3l;
    private LazyEdgeProvider sut;

    private final Map<String, Set<Long>> retrieverMap = ImmutableMap.of("A", ImmutableSet.of(edgeA1), "B", ImmutableSet.of(edgeB1));

    @BeforeEach
    void stubAndCreateSut() {
        this.sut = new LazyEdgeProvider(edgeRetriever, false);
        when(edgeRetriever.apply(any())).then(i -> Maps.filterKeys(retrieverMap, l -> {
            final Set<String> labels = i.getArgument(0);
            return labels.isEmpty() || labels.contains(l);
        }));
    }

    @Test
    void provideEdges() {
        // when: 'requesting an edge for which the label is not retrieved'
        final Collection<Long> actualEdgeAs = sut.provideEdges("A");
        // then: 'the single A1 edge is returned'
        assertEdges(actualEdgeAs, edgeA1);
        verify(edgeRetriever, times(1)).apply(eq(ImmutableSet.of("A")));
        // when: 'requesting the edges again'
        sut.provideEdges("A");
        // then: 'the edges are cached'
        verify(edgeRetriever, times(1)).apply(eq(ImmutableSet.of("A")));

        // when: 'requesting all edges'
        final Iterable<Long> actualEdgeAll = sut.provideEdges();
        // then: 'the all edges are returned'
        assertEdges(actualEdgeAll, edgeA1, edgeB1);
        // and: 'the load invocations match'
        verify(edgeRetriever, times(1)).apply(eq(ImmutableSet.of()));
    }

    @Test
    void registerEdge() {
        // setup: 'a cached edge'
        sut.register("B", edgeB2);
        // when: 'requesting an edge for which the label is not retrieved'
        final Iterable<Long> actualEdgeAs = sut.provideEdges("B");
        // then: 'the single A1 edge is returned'
        assertEdges(actualEdgeAs, edgeB1, edgeB2);
        verify(edgeRetriever, times(1)).apply(eq(ImmutableSet.of("B")));
    }

    @Test
    void completeLyLoaded() {
        // setup: 'a completely loaded provider'
        this.sut = new LazyEdgeProvider(edgeRetriever, true);
        // when: 'requesting an edge for which the label is not retrieved'
        final Iterable<Long> actualEdgeAs = sut.provideEdges("B");
        // then: 'no edges are returned'
        assertEdges(actualEdgeAs);
        // when: 'adding an edge'
        sut.register("B", edgeB2);
        // then: 'the edge is returned for its label'
        assertEdges(sut.provideEdges("B"), edgeB2);
        // and: 'no interactions with the retriever'
        verify(edgeRetriever, never()).apply(any());

    }

    private void assertEdges(final Iterable<Long> actual, final Long... expected) {
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(actual));
    }

}