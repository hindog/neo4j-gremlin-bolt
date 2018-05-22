package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.features.Neo4JFeatures;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JVertexTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JElementStateScope<Neo4JVertexState> scope;

    @Mock
    private EdgeProvider inProvider, outProvider;

    @Mock
    private Neo4JVertex otherVertex, inVertex, outVertex;

    @Mock
    private Vertex invalidVertex;

    @Mock
    private Neo4JEdge inEdge, outEdge;

    private final Neo4JVertexState state = new Neo4JVertexState(ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b", "x", "y"));

    private Neo4JVertex sut;

    @BeforeEach
    void createSutAndStub() {
        this.sut = new Neo4JVertex(graph, 1l, scope, inProvider, outProvider);
        when(scope.get(1l)).thenReturn(state);
        when(inEdge.id()).thenReturn(1l);
        when(outEdge.id()).thenReturn(2l);
        when(graph.features()).thenReturn(Neo4JFeatures.INSTANCE);
        when(graph.addEdge(eq("edge"), eq(sut), eq(otherVertex), any())).thenReturn(inEdge);
        when(inProvider.provideEdges(any())).thenReturn(ImmutableList.of(1l));
        when(outProvider.provideEdges(any())).thenReturn(ImmutableList.of(2l));
        when(graph.edges(1l)).then(i -> Stream.of(inEdge).iterator());
        when(graph.edges(2l)).then(i -> Stream.of(outEdge).iterator());
        when(graph.edges(1l, 2l)).then(i -> Stream.of(inEdge, outEdge).iterator());
        when(graph.edges(2l, 1l)).then(i -> Stream.of(outEdge, inEdge).iterator());
        when(inEdge.outVertex()).thenReturn(inVertex);
        when(inEdge.inVertex()).thenReturn(sut);
        when(outEdge.inVertex()).thenReturn(sut);
        when(outEdge.outVertex()).thenReturn(outVertex);
    }

    @Test
    void addEdge() {
        // when: 'adding an edge'
        assertEquals(inEdge, sut.addEdge("edge", otherVertex));
        // then: 'the edge is registered in the out provider'
        verify(outProvider, times(1)).register("edge", inEdge.id());
        // expect: 'non neo4j vertices throw exceptions'
        assertThrows(IllegalArgumentException.class, () -> sut.addEdge("x", invalidVertex));
    }

    @Test
    void edges() {
        assertEquals(ImmutableList.of(inEdge), ImmutableList.copyOf(sut.edges(Direction.IN, "x")));
        assertEquals(ImmutableList.of(outEdge), ImmutableList.copyOf(sut.edges(Direction.OUT, "x")));
        assertEquals(ImmutableList.of(inEdge, outEdge), ImmutableList.copyOf(sut.edges(Direction.BOTH, "x")));
    }

    @Test
    void vertices() {
        assertEquals(ImmutableList.of(inVertex), ImmutableList.copyOf(sut.vertices(Direction.IN, "x")));
        assertEquals(ImmutableList.of(outVertex), ImmutableList.copyOf(sut.vertices(Direction.OUT, "x")));
        assertEquals(ImmutableList.of(inVertex, outVertex), ImmutableList.copyOf(sut.vertices(Direction.BOTH, "x")));
    }


    @Test
    void properties() {
        assertEquals(ImmutableList.of("b", "y"),
                ImmutableList.copyOf(sut.properties("a", "x")).stream()
                        .map(Property::value).collect(ImmutableList.toImmutableList()));
    }

    @Test
    void property() {
        assertEquals("y", sut.property("x").value());
        sut.property("a", "b");
        verify(scope, never()).update(anyLong(), any());
    }

    @Test
    void labels() {
        assertEquals("x::y", sut.label());
    }

    @Test
    void checkEquals() {
        assertFalse(sut.equals(inEdge));
        assertTrue(sut.equals(sut));
    }

    @Test
    void checkToString() {
        assertEquals(StringFactory.vertexString(sut), sut.toString());
    }

}