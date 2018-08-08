package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JEdgeTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JElementStateScope<Neo4JEdgeState, AbstractQueryBuilder> scope;

    @Mock
    private Neo4JVertex inV, outV;

    private final Neo4JEdgeState state = new Neo4JEdgeState("x", ImmutableMap.of("a", "b", "x", "y"), 1l, 2l);

    private Neo4JEdge sut;

    @BeforeEach
    void createSutAndStub() {
        this.sut = new Neo4JEdge(graph, 1l, scope);
        when(scope.get(1l)).thenReturn(state);
        when(graph.vertices(1l)).then(i -> Stream.of(inV).iterator());
        when(graph.vertices(2l)).then(i -> Stream.of(outV).iterator());
        when(graph.vertices(1l, 2l)).then(i -> Stream.of(inV, outV).iterator());
        when(graph.vertices(2l, 1l)).then(i -> Stream.of(outV, inV).iterator());
        when(inV.id()).thenReturn(1l);
        when(outV.id()).thenReturn(2l);
    }

    @Test
    void vertices() {
        assertEquals(ImmutableList.of(inV), ImmutableList.copyOf(sut.vertices(Direction.IN)));
        assertEquals(ImmutableList.of(outV), ImmutableList.copyOf(sut.vertices(Direction.OUT)));
        assertEquals(ImmutableList.of(inV, outV), ImmutableList.copyOf(sut.vertices(Direction.BOTH)));
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
    void label() {
    }

    @Test
    void checkEquals() {
        assertFalse(sut.equals(inV));
        assertTrue(sut.equals(sut));
    }

    @Test
    void checkToString() {
        assertEquals(StringFactory.edgeString(sut), sut.toString());
    }

}