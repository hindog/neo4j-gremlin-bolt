package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.testutils.StatementExecutorStub;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultRelationProviderTest {

    private final Neo4JGraphPartition partition = Neo4JLabelGraphPartition.allLabelsOf("graphLabel");
    private final StatementExecutorStub stub = new StatementExecutorStub();
    private final RelationProvider sut = new DefaultRelationProvider(stub, partition);

    @Test
    void loadRelatedIds() {
        stub.stubEdgeLoad("MATCH (n:`graphLabel`)-[r:`funny`]->(m:`graphLabel`) WHERE ID(n) IN {vertexId1} RETURN r",
                ImmutableMap.of("vertexId1", Collections.singleton(2l)), 3l, 1l, 2l);
        final Map<String, Set<Long>> actual = sut.loadRelationIds(2l, Direction.OUT, ImmutableSet.of("funny"));
        assertEquals(ImmutableMap.of("remote", Collections.singleton(3l)), actual);
    }
}