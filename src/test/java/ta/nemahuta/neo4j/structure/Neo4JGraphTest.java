package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.cache.SessionCache;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.testutils.StatementExecutorStub;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
        when(sessionCache.getEdgeCache()).thenReturn(edgeCache);
        this.sut = new Neo4JGraph(session, sessionCache, graphPartition, configuration);
        when(session.beginTransaction()).thenReturn(transaction);
        when(configuration.toApacheConfiguration()).thenReturn(apacheConfiguration);
        when(transaction.run(any(Statement.class))).then(i -> stub.executeStatement(i.getArgument(0)));
    }

    @Test
    void addVertex() {
        stub.stubVertexCreate("CREATE (v:`x`:`y`) SET v={vertexProps1} RETURN ID(v)", ImmutableMap.of("vertexProps1", ImmutableMap.of("x", "y")), 1l);
        assertTrue(sut.addVertex(T.label, "x::y", "x", "y") instanceof Neo4JVertex);
    }

    @Test
    void compute() {
    }

    @Test
    void compute1() {
    }

    @Test
    void vertices() {
    }

    @Test
    void edges() {
    }

    @Test
    void tx() {
    }

    @Test
    void close() {
    }

    @Test
    void variables() {
    }

    @Test
    void configuration() {
    }

    @Test
    void features() {
    }
}