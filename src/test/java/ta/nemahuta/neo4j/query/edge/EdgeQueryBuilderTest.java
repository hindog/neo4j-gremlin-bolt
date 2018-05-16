package ta.nemahuta.neo4j.query.edge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ta.nemahuta.neo4j.testutils.MockUtils.mockProperties;

class EdgeQueryBuilderTest extends AbstractStatementBuilderTest {

    @Test
    void buildLhsLabelsAndRelationLabelsReturningId() {
        assertBuildsStatement("MATCH (n:`u`:`v`:`graphLabel`)<-[r:`x`]-(m:`graphLabel`) RETURN r.id",
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
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`x`]->(m:`u`:`v`:`graphLabel`) RETURN r.id",
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
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`x`]->(m:`graphLabel`) WHERE n.id={vertexId1} AND m.id={vertexId2} RETURN n.id, r, m.id",
                ImmutableMap.of("vertexId1", 1l, "vertexId2", 2l),
                query()
                        .labels(ImmutableSet.of("x"))
                        .direction(Direction.OUT)
                        .where(v -> v.getLhs().id(new Neo4JPersistentElementId<>(1l)).and(v.getRhs().id(new Neo4JPersistentElementId<>(2l))))
                        .andThen(e -> e.returnEdge())
        );
    }

    @Test
    void queryRelationLabelsReturnId() {
        assertBuildsStatement("MATCH (n:`graphLabel`)<-[r:`x`|:`y`]-(m:`graphLabel`) RETURN r.id",
                Collections.emptyMap(),
                query()
                        .labels(ImmutableSet.of("x", "y"))
                        .direction(Direction.IN)
                        .andThen(e -> e.returnId())
        );

    }

    @Test
    void createEdgeWithVertexIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE n.id={vertexId1} AND m.id={vertexId2} CREATE (n)-[r:`u`={edgeProps1}]->(m) RETURN r.id",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeProps1", ImmutableMap.of("cool", "cat")),
                query()
                        .where(v -> v.getLhs().id(new Neo4JPersistentElementId<>(1l)).and(v.getRhs().id(new Neo4JPersistentElementId<>(2l))))
                        .labels(ImmutableSet.of("x", "y"))
                        .andThen(e -> e.createEdge(new Neo4JTransientElementId<>(1l), Direction.OUT, "u", mockProperties(ImmutableMap.of("cool", "cat"))))

        );
    }

    @Test
    void createEdgeWithRemoteIdAndVertexIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`), (m:`graphLabel`) WHERE n.id={vertexId1} AND m.id={vertexId2} CREATE (n)-[r:`u`={edgeProps1}]->(m)",
                ImmutableMap.of("vertexId2", 2l, "vertexId1", 1l, "edgeProps1", ImmutableMap.of("cool", "cat", "id", 3l)),
                query()
                        .where(v -> v.getLhs().id(new Neo4JPersistentElementId<>(1l)).and(v.getRhs().id(new Neo4JPersistentElementId<>(2l))))
                        .labels(ImmutableSet.of("x", "y"))
                        .andThen(e -> e.createEdge(new Neo4JPersistentElementId<>(3l), Direction.OUT, "u", mockProperties(ImmutableMap.of("cool", "cat"))))

        );
    }

    @Test
    void deleteWithId() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`a`]->(m:`graphLabel`) WHERE r.id={edgeId1} DETACH DELETE r",
                ImmutableMap.of("edgeId1", 1l),
                query()
                        .where(v -> v.whereId(new Neo4JPersistentElementId<>(1l)))
                        .direction(Direction.OUT)
                        .labels(Collections.singleton("a"))
                        .andThen(e -> e.deleteEdge())

        );
    }

    @Test
    void deleteWithIds() {
        assertBuildsStatement("MATCH (n:`graphLabel`)-[r:`a`|:`b`]->(m:`graphLabel`) WHERE r.id IN {edgeId1} DETACH DELETE r",
                ImmutableMap.of("edgeId1", ImmutableList.of(1l, 2l)),
                query()
                        .where(v -> v.whereIds(ImmutableSet.of(new Neo4JPersistentElementId<>(1l), new Neo4JPersistentElementId<>(2l))))
                        .direction(Direction.OUT)
                        .labels(ImmutableSet.of("a", "b"))
                        .andThen(e -> e.deleteEdge())

        );
    }

    @Test
    void updatePropertiesWithId() {
        final Map<String, Object> properties = new HashMap<>(ImmutableMap.of("a", "c", "e", "f"));
        properties.put("x", null);

        assertBuildsStatement("MATCH (n:`graphLabel`)-[r]-(m:`graphLabel`) WHERE n.id={vertexId1} AND m.id={vertexId2} AND r.id={edgeId1} SET r={edgeProps1}",
                ImmutableMap.of("vertexId1", 1l, "vertexId2", 2l, "edgeId1", 3l, "edgeProps1", properties),
                query()
                        .where(b -> b.getLhs().id(new Neo4JPersistentElementId<>(1l))
                                .and(b.getRhs().id(new Neo4JPersistentElementId<>(2l)).and(b.whereId(new Neo4JPersistentElementId<>(3l)))))
                        .direction(Direction.BOTH)
                        .andThen(b -> b.properties(
                                mockProperties(ImmutableMap.of("x", "y", "a", "b", "u", "v")),
                                mockProperties(ImmutableMap.of("a", "c", "e", "f", "u", "v"))
                                )
                        )
        );
    }

    private static EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.allLabelsOf("graphLabel"), new Neo4JNativeElementIdAdapter());
    }

}
