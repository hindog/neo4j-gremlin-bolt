package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.Statement;
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

import java.util.*;
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

    private final Map<Pair<String, Map<String, Object>>, StatementResult> statementStubs = new HashMap<>();

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
        when(statementExecutor.executeStatement(any())).then(i -> {
            final Statement stmt = i.getArgument(0);
            if (stmt == null) {
                return null; // While stubbing, we obviously get nulls, yeah
            }
            final Pair<String, Object> key = new Pair<>(stmt.text(), stmt.parameters().asObject());
            return Optional.ofNullable(statementStubs.get(key))
                    .orElseThrow(() -> new IllegalStateException("No stub exists for statement:\n" +
                            key +
                            "\nin\n - " +
                            statementStubs.keySet().stream().map(Object::toString).collect(Collectors.joining("\n - "))));
        });

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
        stubVertexAndEdgeIdFindInBound(2l);
        // and: 'the edge load operation'
        stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l));
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.inEdgesOf(graph, vertex2, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void outEdgeOf() {
        // setup: 'the edge match id operation'
        stubVertexAndEdgeIdFindOutBound(2l);
        // and: 'the edge load operation'
        stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l));
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.outEdgesOf(graph, vertex2, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void getOrLoadLabelIn() {
        // setup: 'the edge match id operation'
        stubVertexAndEdgeIdFindOutBound(null);
        stubVertexAndEdgeIdFindInBound(null);
        // and: 'the edge load operation'
        stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l));
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JEdge> actual = sut.getOrLoadLabelIn(graph, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedEdge(actual);
    }

    @Test
    void loadEdge() {
        // setup: 'the edge load operation'
        stubEdgeLoad("MATCH (n:`graphName`)-[r]->(m:`graphName`) WHERE ID(r)={edgeId1} RETURN ID(n), r, ID(m)", ImmutableMap.of("edgeId1", 2l));
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
        stubStatementExecution("MATCH (n:`graphName`), (m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} CREATE (n)-[r:`transient`={edgeProps1}]->(m) RETURN ID(r)",
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
        stubStatementExecution("MATCH (n:`graphName`)-[r]-(m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} AND ID(r)={edgeId1} SET r={edgeProps1}",
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
        stubStatementExecution("MATCH (n:`graphName`)-[r:`sync`]-(m:`graphName`) WHERE ID(n)={vertexId1} AND ID(m)={vertexId2} AND ID(r)={edgeId1} DETACH DELETE r",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeId1", 1l), mock(StatementResult.class));
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
    }

    private void stubVertexAndEdgeIdFindOutBound(Long id) {
        stubVertexAndEdgeIdFind("MATCH (n:`graphName`)-[r:`a`|:`b`]->(m:`graphName`) " + (id != null ? "WHERE ID(n)={vertexId1} " : "") + "RETURN ID(r)",
                id != null ? ImmutableMap.of("vertexId1", 2l) : ImmutableMap.of());
    }

    private void stubVertexAndEdgeIdFindInBound(Long id) {
        stubVertexAndEdgeIdFind("MATCH (n:`graphName`)<-[r:`a`|:`b`]-(m:`graphName`) " + (id != null ? "WHERE ID(n)={vertexId1} " : "") + "RETURN ID(r)",
                id != null ? ImmutableMap.of("vertexId1", 2l) : ImmutableMap.of());
    }

    private void assertLoadedEdge(final Stream<Neo4JEdge> actualStream) {
        final List<Neo4JEdge> actuals = actualStream.collect(Collectors.toList());
        assertEquals(1, actuals.size());
        final Neo4JEdge actual = actuals.iterator().next();
        assertEquals("remote", actual.label());
        assertEquals("y", actual.property("x").value());
        assertEquals(vertex1, actual.inVertex());
        assertEquals(vertex2, actual.outVertex());

    }

    private void stubVertexAndEdgeIdFind(final String text, final Map<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, 2l)
        )));
    }

    private void stubEdgeLoad(final String text, final Map<String, Object> params) {
        stubStatementExecution(text, params, mockStatementResult(mockRecord(
                mockValue(Value::asObject, null, vertex1Id),
                mockValue(Value::asRelationship, null, mockRelationship(2l, "remote", ImmutableMap.of("x", "y"))),
                mockValue(Value::asObject, null, vertex2Id)
        )));
    }

    private void stubStatementExecution(final String text, final Map<String, Object> params, final StatementResult statementResult) {
        statementStubs.put(new Pair<>(text, params), statementResult);
    }

}