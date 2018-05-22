package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Neo4JEdgeStateHandlerTest extends AbstractStatementBuilderTest {

    @Mock
    private StatementExecutor executor;

    @Mock
    private Relationship relationship;

    private Neo4JGraphPartition partition = Neo4JLabelGraphPartition.allLabelsOf("graphLabel");

    private Neo4JEdgeStateHandler sut;

    private final long inId = 1l, outId = 2l, id = 3l;
    private final String label = "yay";
    private final ImmutableMap<String, Object> properties = ImmutableMap.of("a", "b"),
            newProperties = ImmutableMap.of("b", "c");
    private final Neo4JEdgeState state = new Neo4JEdgeState(label, properties, inId, outId),
            newState = new Neo4JEdgeState(label, newProperties, inId, outId);

    @BeforeEach
    void createStateHandler() {
        this.sut = new Neo4JEdgeStateHandler(executor, partition);
    }

    @Test
    void getIdAndConvertToState() {
        // setup: 'stubbing the relationship'
        when(relationship.asMap()).thenReturn(properties);
        when(relationship.type()).thenReturn(label);
        when(relationship.startNodeId()).thenReturn(inId);
        when(relationship.endNodeId()).thenReturn(outId);
        when(relationship.id()).thenReturn(id);
        // when: 'converting the state'
        final Pair<Long, Neo4JEdgeState> state = sut.getIdAndConvertToState(MockUtils.mockRecord(MockUtils.mockValue(Value::asRelationship, null, relationship)));
        assertEquals(new Pair<>(3l, this.state), state);
    }

    @Test
    void createDeleteCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE ID(r) IN {edgeId1} DETACH DELETE r",
                ImmutableMap.of("edgeId1", Collections.singleton(1l)),
                sut.createDeleteCommand(1l));
    }

    @Test
    void createUpdateCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE ID(r) IN {edgeId1} SET r={edgeProps1}",
                ImmutableMap.of("edgeId1", Collections.singleton(1l), "edgeProps1", newProperties),
                sut.createUpdateCommand(1l, state, newState));
    }

    @Test
    void createInsertCommand() {
        assertStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} CREATE (n)-[r:`yay`]->(m) SET r={edgeProps1} RETURN ID(r)",
                ImmutableMap.of("edgeProps1", properties, "vertexId1", Collections.singleton(outId), "vertexId2", Collections.singleton(inId)),
                sut.createInsertCommand(state));
    }

    @Test
    void createLoadCommand() {
        assertStatement("MATCH (n:`graphLabel`)-[r]->(m:`graphLabel`) WHERE ID(r) IN {edgeId1} RETURN r",
                ImmutableMap.of("edgeId1", ImmutableSet.of(1l, 2l)),
                sut.createLoadCommand(ImmutableSet.of(1l, 2l)));
    }
}