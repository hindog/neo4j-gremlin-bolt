package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.id.*;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JVertex;
import ta.nemahuta.neo4j.structure.VertexOnEdgeSupplier;
import ta.nemahuta.neo4j.testutils.MockUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ta.nemahuta.neo4j.testutils.MockUtils.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultNeo4JEdgeScopeTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private StatementExecutor statementExecutor;

    @Mock
    private Neo4JElementScope<Neo4JVertex> vertexScope;

    @Mock
    private Neo4JVertex vertex1, vertex2;

    private DefaultNeo4JEdgeScope sut;

    private Neo4JEdge edgeSync, edgeTransient, edgeRemote;
    private final Neo4JElementId<?> vertex1Id = new Neo4JPersistentElementId<>(1l),
            vertex2Id = new Neo4JPersistentElementId<>(2l);

    @BeforeEach
    void createSutAndStub() {
        sut = new DefaultNeo4JEdgeScope(new Neo4JNativeElementIdAdapter(), statementExecutor, Neo4JLabelGraphPartition.allLabelsOf("graphName"), vertexScope);

        when(vertex1.id()).thenReturn((Neo4JElementId) vertex1Id);
//        when(vertex1.toString()).thenReturn("vertex 1");
        when(vertex2.id()).thenReturn((Neo4JElementId) vertex2Id);
//        when(vertex2.toString()).thenReturn("vertex 2");
        edgeSync = new Neo4JEdge(graph, new Neo4JPersistentElementId<>(1l),
                ImmutableSet.of("sync"),
                Optional.of(MockUtils.mockMapAccessor(ImmutableMap.of("a", "b"))),
                sut.getPropertyFactory(),
                VertexOnEdgeSupplier.wrap(vertex1),
                VertexOnEdgeSupplier.wrap(vertex2)
        );
        edgeTransient = new Neo4JEdge(graph, new Neo4JTransientElementId<>(1l),
                ImmutableSet.of("transient"),
                Optional.empty(),
                sut.getPropertyFactory(),
                VertexOnEdgeSupplier.wrap(vertex1),
                VertexOnEdgeSupplier.wrap(vertex2)
        );
        edgeRemote = new Neo4JEdge(graph, new Neo4JPersistentElementId<>(2l),
                ImmutableSet.of("remote"),
                Optional.of(MockUtils.mockMapAccessor(ImmutableMap.of("a", "b"))),
                sut.getPropertyFactory(),
                sut.createVertexGet(graph, vertex2.id()),
                sut.createVertexGet(graph, vertex1.id())
        );
        final Neo4JNativeElementIdAdapter vertexIdAdapter = new Neo4JNativeElementIdAdapter();
        when(vertexScope.getIdAdapter()).thenReturn((Neo4JElementIdAdapter) vertexIdAdapter);
        when(graph.tx()).thenReturn(transaction);
    }

    @Test
    void loadEdgeInScope() {
        // setup: 'the edge in the scope and the load operation'
        sut.add(edgeSync);
        // when: 'loading the edge'
        final Stream<Neo4JEdge> actual = sut.getOrLoad(graph, Stream.of(edgeSync.id()).iterator());
        // then: 'the result is the edge from the scope'
        assertEquals(ImmutableSet.of(edgeSync), actual.collect(ImmutableSet.toImmutableSet()));
        // and: 'no statements have been fired'
        verify(statementExecutor, never()).executeStatement(any());
    }

    @Test
    void loadEdge() {
        // setup: 'stubbing loading the elements'
        final Map<Neo4JElementId<?>, List<Neo4JVertex>> vertexMap = Stream.of(vertex1, vertex2).collect(Collectors.groupingBy(njv -> njv.id()));
        when(vertexScope.getOrLoad(eq(graph), any(Iterator.class)))
                .thenAnswer(i -> vertexMap.get((i.<Iterator<Neo4JElementId<?>>>getArgument(1)).next()).stream());
        final StatementResult statementResult = mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, vertex1Id),
                mockValue(Value::asRelationship, null, mockRelationship(2l, "remote", ImmutableMap.of("x", "y"))),
                mockValue(Value::asObject, null, vertex2Id)
        ));
        when(statementExecutor.executeStatement(argThat(a ->
                a.text().equals("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE r.id={edgeId1} RETURN n.id, r, m.id") && a.parameters().asMap().equals(ImmutableMap.of("edgeId1", 2l))
        ))).thenReturn(statementResult);

        final List<Neo4JEdge> actuals = sut.getOrLoad(graph, Stream.of(edgeRemote.id()).iterator()).collect(Collectors.toList());
        assertEquals(1, actuals.size());
        final Neo4JEdge actual = actuals.iterator().next();
        assertEquals("remote", actual.label());
        assertEquals("y", actual.property("x").value());
        assertEquals(vertex1, actual.inVertex());
        assertEquals(vertex2, actual.outVertex());
    }

}