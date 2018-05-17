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
import ta.nemahuta.neo4j.features.Neo4JFeatures;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.Neo4JSession;
import ta.nemahuta.neo4j.structure.*;
import ta.nemahuta.neo4j.testutils.MockUtils;
import ta.nemahuta.neo4j.testutils.StatementExecutorStub;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ta.nemahuta.neo4j.testutils.MockUtils.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultNeo4JVertexScopeTest {
    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Neo4JSession session;

    private final StatementExecutorStub statementExecutor = spy(new StatementExecutorStub());

    @Mock
    private Neo4JEdge edge1, edge2;

    @Mock
    private EdgeProvider inEdgeProvider, outEdgeProvider;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EdgeFactory edgeFactory;

    private DefaultNeo4JVertexScope sut;

    private Neo4JVertex vertexSync, vertexTransient;

    private final Neo4JElementId<?> edge1Id = new Neo4JPersistentElementId<>(1l),
            edge2Id = new Neo4JPersistentElementId<>(2l);

    @BeforeEach
    void createSutAndStub() {
        sut = new DefaultNeo4JVertexScope(new Neo4JNativeElementIdAdapter(), statementExecutor, Neo4JLabelGraphPartition.allLabelsOf("graphName"));

        when(edge1.id()).thenReturn((Neo4JElementId) edge1Id);
        when(edge2.id()).thenReturn((Neo4JElementId) edge2Id);
        vertexSync = new Neo4JVertex(graph, new Neo4JPersistentElementId<>(1l),
                ImmutableSet.of("sync"),
                Optional.of(MockUtils.mockMapAccessor(ImmutableMap.of("a", "b"))),
                sut.getPropertyFactory(),
                (v) -> inEdgeProvider,
                (v) -> outEdgeProvider,
                edgeFactory
        );
        vertexTransient = new Neo4JVertex(graph, new Neo4JTransientElementId<>(1l),
                ImmutableSet.of("transient"),
                Optional.empty(),
                sut.getPropertyFactory(),
                (v) -> inEdgeProvider,
                (v) -> outEdgeProvider,
                edgeFactory
        );
        when(graph.features()).thenReturn(Neo4JFeatures.INSTANCE);
        when(graph.tx()).thenReturn(transaction);
        when(graph.getSession()).thenReturn(session);
    }

    @Test
    void loadVertexInScope() {
        // setup: 'the edge in the scope and the load operation'
        sut.add(vertexSync);
        // when: 'loading the edge'
        final Stream<Neo4JVertex> actual = sut.getOrLoad(graph, Stream.of(vertexSync.id()).iterator());
        // then: 'the result is the edge from the scope'
        assertEquals(ImmutableSet.of(vertexSync), actual.collect(ImmutableSet.toImmutableSet()));
        // and: 'no statements have been fired'
        verify(statementExecutor, never()).executeStatement(any());
    }

    @Test
    void getOrLoadLabelIn() {
        // setup: 'the edge match id operation'
        statementExecutor.stubVertexIdForLabel("a");
        statementExecutor.stubVertexIdForLabel("b");
        statementExecutor.stubVertexLoad("MATCH (v:`graphName`) WHERE ID(v)={vertexId1} RETURN v", ImmutableMap.of("vertexId1", 2l));
        // and: 'the edge load operation'
        // when: 'getting the in edges of vertex 2'
        final Stream<Neo4JVertex> actual = sut.getOrLoadLabelIn(graph, ImmutableSet.of("a", "b"));
        // then: 'the loaded edge is built correctly'
        assertLoadedVertex(actual);
    }

    @Test
    void commitTransient() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(vertexTransient);
        vertexTransient.property("x", "y");
        statementExecutor.stubStatementExecution("CREATE (v:`transient`:`graphName`={vertexProps1}) RETURN ID(v)",
                ImmutableMap.of("vertexProps1", ImmutableMap.of("x", "y")),
                mockStatementResult(mockRecord(mockValue(Value::asObject, null, 3l)))
        );
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
        // and: 'the identifier has changed'
        assertEquals(new Neo4JPersistentElementId<>(3l), vertexTransient.id());
    }

    @Test
    void commitModified() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(vertexSync);
        vertexSync.property("a").remove();
        vertexSync.property("c", "d");
        statementExecutor.stubStatementExecution("MATCH (v:`sync`:`graphName`) WHERE ID(v)={vertexId1} SET v:`graphName` SET v={vertexProps1}",
                ImmutableMap.of("vertexId1", 1l, "vertexProps1", ImmutableMap.of("c", "d")), mock(StatementResult.class));
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
    }

    @Test
    void commitDeleted() {
        // setup: 'the synchronized element in the scope being modified'
        sut.add(vertexSync);
        vertexSync.remove();
        statementExecutor.stubStatementExecution("MATCH (v:`sync`:`graphName`) WHERE ID(v)={vertexId1} DETACH DELETE v",
                ImmutableMap.of("vertexId1", 1l), mock(StatementResult.class));
        // when: 'committing the scope'
        sut.commit();
        // then: 'the statement should be executed'
        verify(statementExecutor).executeStatement(any());
    }

    private void assertLoadedVertex(@Nonnull final Stream<Neo4JVertex> actualStream) {
        final List<Neo4JVertex> actuals = actualStream.collect(Collectors.toList());
        assertEquals(1, actuals.size());
        final Neo4JVertex actual = actuals.iterator().next();
        assertEquals("remote", actual.label());
        assertEquals("y", actual.property("x").value());
    }


}