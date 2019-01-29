package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.testutils.MockUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JVertexStateHandlerTest extends AbstractStatementBuilderTest {

    @Mock
    private StatementExecutor executor;

    @Mock
    private Node node;

    private Neo4JGraphPartition partition = Neo4JLabelGraphPartition.allLabelsOf("graphLabel");

    private Neo4JVertexStateHandler sut;

    private final long id = 1l;
    private final ImmutableSet<String> labels = ImmutableSet.of("a", "b"),
            newLabels = ImmutableSet.of("c", "d");
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("a", "b"),
            newProperties = ImmutableMap.of("b", "c");
    private final Neo4JVertexState state = new Neo4JVertexState(labels, properties),
            newState = new Neo4JVertexState(newLabels, newProperties);

    @BeforeEach
    void createStateHandler() {
        this.sut = new Neo4JVertexStateHandler(executor, partition);
    }

    @Test
    void getIdAndConvertToState() {
        // setup: 'stubbing the node'
        when(node.asMap()).thenReturn(properties);
        when(node.id()).thenReturn(id);
        when(node.labels()).thenReturn(partition.ensurePartitionLabelsSet(labels));
        // when: 'converting the state'
        final Pair<Long, Neo4JVertexState> state = sut.getIdAndConvertToState(MockUtils.mockRecord(MockUtils.mockValue(Value::asNode, null, node)));
        assertEquals(new Pair<>(id, this.state), state);
    }

    @Test
    void createDeleteCommand() {
        assertStatement("MATCH (v:`graphLabel`) WHERE ID(v) IN {vertexId1} DETACH DELETE v",
                ImmutableMap.of("vertexId1", Collections.singleton(id)),
                sut.createDeleteCommand(1l));
    }

    @Test
    void createUpdateCommand() {
        assertStatement("MATCH (v:`graphLabel`) WHERE ID(v) IN {vertexId1} SET v:`graphLabel`:`c`:`d` REMOVE v:`a`:`b` SET v={vertexProps1}",
                ImmutableMap.of("vertexId1", Collections.singleton(id), "vertexProps1", newProperties),
                sut.createUpdateCommand(1l, state, newState));
    }

    @Test
    void createInsertCommand() {
        assertStatement("CREATE (v:`a`:`b`:`graphLabel`) SET v={vertexProps1} RETURN ID(v)",
                ImmutableMap.of("vertexProps1", properties),
                sut.createInsertCommand(state));
    }

    @Test
    void createLoadCommand() {
        assertStatement("MATCH (v:`graphLabel`) WHERE ID(v) IN {vertexId1} RETURN v",
                ImmutableMap.of("vertexId1", ImmutableSet.of(2l, 3l)),
                sut.createLoadCommand(ImmutableSet.of(2l, 3l)));
    }

    @Test
    void createCreateIndexCommand() {
        assertStatement("CREATE INDEX ON :`x`(y,z)",
                ImmutableMap.of(),
                sut.createCreateIndexCommand("x", ImmutableSet.of("y", "z")));
    }
}