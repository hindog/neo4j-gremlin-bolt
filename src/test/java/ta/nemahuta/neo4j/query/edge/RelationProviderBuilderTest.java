package ta.nemahuta.neo4j.query.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import java.util.Collections;

class RelationProviderBuilderTest extends AbstractStatementBuilderTest {

    @Test
    void buildLhsLabelsAndRelationLabelsReturningId() {
        assertBuildsStatement("MATCH (n:`u`:`v`:`graphLabel`)<-[r:`x`]-(m:`graphLabel`) RETURN ID(r)",
                Collections.emptyMap(),
                query()
                        .lhsMatch(v -> v.labelsMatch(ImmutableSet.of("u", "v")))
                        .labels(ImmutableSet.of("x"))
                        .direction(Direction.IN)
                        .andThen(e -> e.returnId())
        );
    }

    @Test
    void buildRhsLabelsAndRelationLabelsReturningId() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`x`]->(m:`u`:`v`:`graphLabel`) RETURN ID(r)",
                Collections.emptyMap(),
                query()
                        .rhsMatch(v -> v.labelsMatch(ImmutableSet.of("u", "v")))
                        .labels(ImmutableSet.of("x"))
                        .direction(Direction.OUT)
                        .andThen(e -> e.returnId())
        );
    }

    @Test
    void buildBothIdsAndRelationLabelsReturningRelation() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`x`]->(m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} RETURN r",
                ImmutableMap.of("vertexId1", Collections.singleton(1l), "vertexId2", Collections.singleton(2l)),
                query()
                        .labels(ImmutableSet.of("x"))
                        .direction(Direction.OUT)
                        .where(v -> v.getLhs().id(1l).and(v.getRhs().id(2l)))
                        .andThen(e -> e.returnEdge())
        );
    }

    @Test
    void queryRelationLabelsReturnId() {
        assertBuildsStatement("MATCH (n:`graphLabel`)<-[r:`x`|:`y`]-(m:`graphLabel`) RETURN ID(r)",
                Collections.emptyMap(),
                query()
                        .labels(ImmutableSet.of("x", "y"))
                        .direction(Direction.IN)
                        .andThen(e -> e.returnId())
        );

    }

    @Test
    void createEdgeWithVertexIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} CREATE (n)-[r:`u`]->(m) SET r={edgeProps1} RETURN ID(r)",
                ImmutableMap.of("vertexId2", Collections.singleton(2l), "vertexId1", Collections.singleton(1l), "edgeProps1", ImmutableMap.of("cool", "cat")),
                query()
                        .where(v -> v.getLhs().id(1l).and(v.getRhs().id(2l)))
                        .labels(ImmutableSet.of("x", "y"))
                        .andThen(e -> e.createEdge(Direction.OUT, "u", ImmutableMap.of("cool", "cat")))

        );
    }

    @Test
    void createEdgeWithRemoteIdAndVertexIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} CREATE (n)-[r:`u`]->(m) SET r={edgeProps1} RETURN ID(r)",
                ImmutableMap.of("vertexId2", Collections.singleton(2l), "vertexId1", Collections.singleton(1l), "edgeProps1", ImmutableMap.of("cool", "cat")),
                query()
                        .where(v -> v.getLhs().id(1l).and(v.getRhs().id(2l)))
                        .labels(ImmutableSet.of("x", "y"))
                        .andThen(e -> e.createEdge(Direction.OUT, "u", ImmutableMap.of("cool", "cat")))

        );
    }

    @Test
    void deleteWithIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`a`|:`b`]->(m:`graphLabel`) WHERE ID(r) IN {edgeId1} DETACH DELETE r",
                ImmutableMap.of("edgeId1", ImmutableList.of(1l, 2l)),
                query()
                        .where(v -> v.whereIds(ImmutableSet.of(1l, 2l)))
                        .direction(Direction.OUT)
                        .labels(ImmutableSet.of("a", "b"))
                        .andThen(e -> e.deleteEdge())

        );
    }

    @Test
    void updatePropertiesWithId() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE ID(n) IN {vertexId1} AND ID(m) IN {vertexId2} AND ID(r) IN {edgeId1} SET r={edgeProps1}",
                ImmutableMap.of("vertexId1", Collections.singleton(1l), "vertexId2", Collections.singleton(2l), "edgeId1", Collections.singleton(3l), "edgeProps1", ImmutableMap.of("a", "c", "e", "f", "u", "v")),
                query()
                        .where(b -> b.getLhs().id(1l)
                                .and(b.getRhs().id(2l).and(b.whereId(3l))))
                        .direction(Direction.BOTH)
                        .andThen(b -> b.properties(
                                ImmutableMap.of("x", "y", "a", "b", "u", "v"),
                                ImmutableMap.of("a", "c", "e", "f", "u", "v")
                                )
                        )
        );
    }

    private static EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(Neo4JLabelGraphPartition.allLabelsOf("graphLabel"));
    }

}
