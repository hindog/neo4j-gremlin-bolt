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
import org.neo4j.driver.v1.types.Relationship;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.testutils.MockUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JEdgeStateHandlerTest extends AbstractStatementBuilderTest {

    @Mock
    private StatementExecutor executor;

    @Mock
    private Relationship relationship;

    private Neo4JGraphPartition partition = Neo4JLabelGraphPartition.allLabelsOf("graphLabel");

    private Neo4JEdgeStateHandler sut;

    private final long endNodeId = 1l, startNodeId = 2l, id = 3l;
    private final String label = "yay";
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("a", "b"),
            newProperties = ImmutableMap.of("b", "c");
    private final Neo4JEdgeState state = new Neo4JEdgeState(label, properties, endNodeId, startNodeId),
            newState = new Neo4JEdgeState(label, newProperties, endNodeId, startNodeId);

    @BeforeEach
    void createStateHandler() {
        this.sut = new Neo4JEdgeStateHandler(executor, partition);
    }

    @Test
    void getIdAndConvertToState() {
        // setup: 'stubbing the relationship'
        when(relationship.asMap()).thenReturn(properties);
        when(relationship.type()).thenReturn(label);
        when(relationship.startNodeId()).thenReturn(startNodeId);
        when(relationship.endNodeId()).thenReturn(endNodeId);
        when(relationship.id()).thenReturn(id);
        // when: 'converting the state'
        final Pair<Long, Neo4JEdgeState> state = sut.getIdAndConvertToState(MockUtils.mockRecord(MockUtils.mockValue(Value::asRelationship, null, relationship)));
        assertEquals(new Pair<>(id, this.state), state);
    }

    @Test
    void createDeleteCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE ID(r) IN {edgeId1} DETACH DELETE r",
                ImmutableMap.of("edgeId1", Collections.singleton(id)),
                sut.createDeleteCommand(id));
    }

    @Test
    void createUpdateCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE ID(r) IN {edgeId1} SET r={edgeProps1}",
                ImmutableMap.of("edgeId1", Collections.singleton(id), "edgeProps1", newProperties),
                sut.createUpdateCommand(id, state, newState));
    }

    @Test
    void createInsertCommand() {
        assertStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} CREATE (n)-[r:`yay`]->(m) SET r={edgeProps1} RETURN ID(r)",
                ImmutableMap.of("edgeProps1", properties, "vertexId1", Collections.singleton(startNodeId), "vertexId2", Collections.singleton(endNodeId)),
                sut.createInsertCommand(state));
    }

    @Test
    void createLoadCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]->(m:`graphLabel`) WHERE ID(r) IN {edgeId1} RETURN r",
                ImmutableMap.of("edgeId1", ImmutableSet.of(4l, 5l)),
                sut.createLoadCommand(ImmutableSet.of(4l, 5l)));
    }

    @Test
    void createCreateIndexCommand() {
        assertStatement("CREATE INDEX ON :`x`(y)",
                ImmutableMap.of(),
                sut.createCreateIndexCommand(ImmutableSet.of("x"), "y"));
    }

    @Test
    void createCreateIndexCommandWrongSize() {
        assertThrows(IllegalArgumentException.class, () -> sut.createCreateIndexCommand(ImmutableSet.of("x", "y"), "z"));
    }

}