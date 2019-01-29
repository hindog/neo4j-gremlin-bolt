package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.cache.SessionCache;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.features.Neo4JFeatures;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.scope.IdCache;
import ta.nemahuta.neo4j.session.Neo4JTransaction;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.testutils.StatementExecutorStub;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JGraphTest {

    @Mock
    private HierarchicalCache<Long, Neo4JEdgeState> edgeCache;

    @Mock
    private HierarchicalCache<Long, Neo4JVertexState> vertexCache;

    @Mock
    private SessionCache sessionCache;

    @Mock
    private Session session;

    @Mock
    private IdCache<Long> knownEdgeIds, knownVertexIds;

    private final Neo4JGraphPartition graphPartition = Neo4JLabelGraphPartition.allLabelsOf("x");

    @Mock
    private Neo4JConfiguration configuration;

    @Mock
    private Transaction transaction;

    @Mock
    private Configuration apacheConfiguration;

    private final StatementExecutorStub stub = new StatementExecutorStub();

    private Neo4JGraph sut;

    @BeforeEach
    void setupSut() {
        when(sessionCache.getVertexCache()).thenReturn(vertexCache);
        when(sessionCache.getKnownVertexIds()).thenReturn(knownVertexIds);
        when(sessionCache.getEdgeCache()).thenReturn(edgeCache);
        when(sessionCache.getKnownEdgeIds()).thenReturn(knownEdgeIds);
        this.sut = new Neo4JGraph(session, sessionCache, graphPartition, configuration);
        when(session.beginTransaction()).thenReturn(transaction);
        when(configuration.toApacheConfiguration()).thenReturn(apacheConfiguration);
        when(transaction.run(any(Statement.class))).then(i -> stub.executeStatement(i.getArgument(0)));
    }

    @Test
    void addVertex() {
        stub.stubVertexCreate("CREATE (v:`z`:`x`) SET v={vertexProps1} RETURN ID(v)", ImmutableMap.of("vertexProps1", ImmutableMap.of("x", "y")), 1l);
        assertTrue(sut.addVertex(T.label, "z", "x", "y") instanceof Neo4JVertex);
        verify(vertexCache, times(1)).put(eq(1l), argThat(state -> state.getLabels().equals(ImmutableSet.of("z"))));
    }

    @Test
    void addEdge() {
        stub.stubVertexCreate("CREATE (v:`y`:`x`) SET v={vertexProps1} RETURN ID(v)", ImmutableMap.of("vertexProps1", ImmutableMap.of("x", "y")), 1l);
        final Vertex outVertex = sut.addVertex(T.label, "y", "x", "y");
        stub.stubVertexCreate("CREATE (v:`z`:`x`) SET v={vertexProps1} RETURN ID(v)", ImmutableMap.of("vertexProps1", ImmutableMap.of("x", "y")), 2l);
        final Vertex inVertex = sut.addVertex(T.label, "z", "x", "y");
        stub.stubEdgeCreate("MATCH (n:`x`), (m:`x`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} CREATE (n)-[r:`q`]->(m) SET r={edgeProps1} RETURN ID(r)",
                ImmutableMap.of("vertexId1", Collections.singleton(1l), "vertexId2", Collections.singleton(2l), "edgeProps1", ImmutableMap.of("q", "p")),
                3l);

        assertTrue(outVertex.addEdge("q", inVertex, "q", "p") instanceof Neo4JEdge);
    }

    @Test
    void compute() {
        assertThrows(UnsupportedOperationException.class, () -> sut.compute());
    }

    @Test
    void compute1() {
        assertThrows(UnsupportedOperationException.class, () -> sut.compute(GraphComputer.class));
    }

    @Test
    void txCommit() {
        sut.tx().commit();
        verify(sessionCache.getEdgeCache(), times(1)).commit();
        verify(sessionCache.getVertexCache(), times(1)).commit();
        verify(sessionCache.getEdgeCache(), times(1)).removeFromParent(any());
        verify(sessionCache.getVertexCache(), times(1)).removeFromParent(any());
    }

    @Test
    void txRollback() {
        sut.tx().rollback();
        verify(sessionCache.getEdgeCache(), times(1)).clear();
        verify(sessionCache.getVertexCache(), times(1)).clear();
    }

    @Test
    void vertices() {
        stub.stubVertexLoad("MATCH (v:`x`) WHERE ID(v) IN {vertexId1} RETURN v", ImmutableMap.of("vertexId1", Collections.singleton(1)), 1l);
        assertEquals(1, ImmutableList.copyOf(sut.vertices(1l)).size());
    }

    @Test
    void verticesAll() {
        stub.stubVertexLoad("MATCH (v:`x`) RETURN v", ImmutableMap.of(), 1l);
        assertEquals(1, ImmutableList.copyOf(sut.vertices()).size());
    }

    @Test
    void edges() {
        stub.stubEdgeLoad("MATCH (n:`x`)-[r]->(m:`x`) WHERE ID(r) IN {edgeId1} RETURN r", ImmutableMap.of("edgeId1", Collections.singleton(1)), 1l, 1l, 2l);
        assertEquals(1, ImmutableList.copyOf(sut.edges(1l)).size());
    }

    @Test
    void createEdgePropertyIndex() {
        stub.stubStatementExecution("CREATE INDEX ON :`relation`(property)", ImmutableMap.of(), mock(StatementResult.class));
        sut.createEdgePropertyIndex("relation", Collections.singleton("property"));
    }

    @Test
    void createVertexPropertyIndex() {
        stub.stubStatementExecution("CREATE INDEX ON :`v1`(property1,property2)", ImmutableMap.of(), mock(StatementResult.class));
        sut.createVertexPropertyIndex("v1", ImmutableSet.of("property1", "property2"));
    }

    @Test
    void tx() {
        assertTrue(sut.tx() instanceof Neo4JTransaction);
    }

    @Test
    void close() {
        // when: 'closing the graph'
        sut.close();
        // then: 'session is closed'
        verify(session).close();
    }

    @Test
    void closeOnOpenTx() {
        // when: 'an open transaction'
        sut.tx().open();
        // then: 'closing the graph'
        assertThrows(IllegalStateException.class, () -> sut.close());
    }

    @Test
    void variables() {
        assertThrows(UnsupportedOperationException.class, () -> sut.variables());
    }

    @Test
    void configuration() {
        assertEquals(apacheConfiguration, sut.configuration());
    }

    @Test
    void features() {
        assertEquals(Neo4JFeatures.INSTANCE, sut.features());
    }
}