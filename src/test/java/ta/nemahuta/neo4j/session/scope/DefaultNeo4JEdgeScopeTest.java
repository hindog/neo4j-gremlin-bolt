package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.id.*;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JVertex;
import ta.nemahuta.neo4j.structure.VertexOnEdgeSupplier;
import ta.nemahuta.neo4j.testutils.MockUtils;
import ta.nemahuta.neo4j.testutils.StatementExecutorStub;

import javax.annotation.Nonnull;
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

    private final StatementExecutorStub statementExecutor = spy(new StatementExecutorStub());

    @Mock
    private Neo4JElementScope<Neo4JVertex> vertexScope;

    @Mock
    private Neo4JVertex vertex1, vertex2;

    private DefaultNeo4JEdgeScope sut;

    private Neo4JEdge edgeSync, edgeTransient;

    private final Neo4JElementId<?> vertex1Id = new Neo4JPersistentElementId<>(1l),
            vertex2Id = new Neo4JPersistentElementId<>(2l);

    @BeforeEach
    void createSutAndStub() {
        sut = new DefaultNeo4JEdgeScope(new Neo4JNativeElementIdAdapter(), statementExecutor, Neo4JLabelGraphPartition.allLabelsOf("graphName"), vertexScope);

        when(vertex1.id()).thenReturn((Neo4JElementId) vertex1Id);
        when(vertex2.id()).thenReturn((Neo4JElementId) vertex2Id);
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
        final Neo4JNativeElementIdAdapter vertexIdAdapter = new Neo4JNativeElementIdAdapter();
        when(vertexScope.getIdAdapter()).thenReturn((Neo4JElementIdAdapter) vertexIdAdapter);
        when(graph.tx()).thenReturn(transaction);
        final Map<Neo4JElementId<?>, List<Neo4JVertex>> vertexMap = Stream.of(vertex1, vertex2).collect(Collectors.groupingBy(njv -> njv.id()));
        when(vertexScope.getOrLoad(eq(graph), any(Iterator.class)))
                .thenAnswer(i -> vertexMap.get((i.<Iterator<Neo4JElementId<?>>>getArgument(1)).next()).stream());
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
    void inEdgeOf() {
        // setup: 'the edge match id operation'
        statementExecutor.stubVertexAndEdgeIdFindInBound(2l);
        // and: 'the edge load operation'
        statementExecutor.stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l), vertex2Id, vertex1Id);
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.inEdgesOf(graph, vertex2, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void outEdgeOf() {
        // setup: 'the edge match id operation'
        statementExecutor.stubVertexAndEdgeIdFindOutBound(2l);
        // and: 'the edge load operation'
        statementExecutor.stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l), vertex2Id, vertex1Id);
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.outEdgesOf(graph, vertex2, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void getOrLoadLabelIn() {
        // setup: 'the edge match id operation'
        statementExecutor.stubVertexAndEdgeIdFindOutBound(null);
        statementExecutor.stubVertexAndEdgeIdFindInBound(null);
        // and: 'the edge load operation'
        statementExecutor.stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l), vertex2Id, vertex1Id);
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.getOrLoadLabelIn(graph, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void loadEdge() {
        // setup: 'the edge load operation'
        statementExecutor.stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l), vertex2Id, vertex1Id);
        // when: 'loading the remote edge'
        final Stream<Neo4JEdge> actual = sut.getOrLoad(graph, Stream.of(new Neo4JPersistentElementId<>(2l)).iterator());
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void commitTransient() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(edgeTransient);
        edgeTransient.property("x", "y");
        statementExecutor.stubStatementExecution("MATCH (n:`graphName`), (m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} CREATE (n)-[r:`transient`={edgeProps1}]->(m) RETURN ID(r)",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeProps1", ImmutableMap.of("x", "y")),
                mockStatementResult(mockRecord(mockValue(Value::asObject, null, 3l)))
        );
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
        // and: 'the identifier has changed'
        assertEquals(new Neo4JPersistentElementId<>(3l), edgeTransient.id());
    }

    @Test
    void commitModified() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(edgeSync);
        edgeSync.property("a").remove();
        edgeSync.property("c", "d");
        statementExecutor.stubStatementExecution("MATCH (n:`graphName`)-[r]-(m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} AND ID(r)={edgeId1} SET r={edgeProps1}",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeId1", 1l, "edgeProps1", ImmutableMap.of("c", "d")), mock(StatementResult.class));
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
    }

    @Test
    void commitDeleted() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(edgeSync);
        edgeSync.remove();
        statementExecutor.stubStatementExecution("MATCH (n:`graphName`)-[r:`sync`]-(m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} AND ID(r)={edgeId1} DETACH DELETE r",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeId1", 1l), mock(StatementResult.class));
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
    }

    private void assertLoadedEdge(@Nonnull final Stream<Neo4JEdge> actualStream) {
        final List<Neo4JEdge> actuals = actualStream.collect(Collectors.toList());
        assertEquals(1, actuals.size());
        final Neo4JEdge actual = actuals.iterator().next();
        assertEquals("remote", actual.label());
        assertEquals("y", actual.property("x").value());
        assertEquals(vertex1, actual.inVertex());
        assertEquals(vertex2, actual.outVertex());

    }

}