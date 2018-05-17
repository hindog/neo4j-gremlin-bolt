package ta.nemahuta.neo4j.query.vertex;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import java.util.Collections;

import static ta.nemahuta.neo4j.testutils.MockUtils.mockProperties;

class VertexQueryBuilderTest extends AbstractStatementBuilderTest {

    @Test
    void buildWhereIdAndLabelsReturnVertex() {
        assertBuildsStatement("MATCH (v:`x`:`y`) WHERE ID(v)={vertexId1} RETURN v",
                ImmutableMap.of("vertexId1", 1l),
                query()
                        .match(q -> q.labelsMatch(ImmutableSet.of("x", "y")))
                        .where(q -> q.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(q -> q.returnVertex())
        );
    }

    @Test
    void createVertexAndReturnId() {
        assertBuildsStatement("CREATE (v:`x`:`y`={vertexProps1}) RETURN ID(v)",
                ImmutableMap.of("vertexProps1", ImmutableMap.of("a", "b")),
                query()
                        .andThen(q -> q.create(new Neo4JTransientElementId<>(2l), ImmutableSet.of("x", "y"), mockProperties(ImmutableMap.of("a", "b"))))
        );
    }

    @Test
    void createVertexAndUseId() {
        assertBuildsStatement("CREATE (v:`x`:`y`={vertexProps1})",
                ImmutableMap.of("vertexProps1", ImmutableMap.of("a", "b")),
                query()
                        .andThen(q -> q.create(new Neo4JPersistentElementId<>(2l), ImmutableSet.of("x", "y"), mockProperties(ImmutableMap.of("a", "b"))))
        );
    }

    @Test
    void deleteVertexById() {
        assertBuildsStatement("MATCH (v) WHERE ID(v)={vertexId1} DETACH DELETE v",
                ImmutableMap.of("vertexId1", 1l),
                query()
                        .match(v -> v.labelsMatch(Collections.emptySet()))
                        .where(v -> v.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(q -> q.delete())
        );
    }

    @Test
    void returnVertexById() {
        assertBuildsStatement("MATCH (v) WHERE ID(v)={vertexId1} RETURN v",
                ImmutableMap.of("vertexId1", 1l),
                query()
                        .match(v -> v.labelsMatch(Collections.emptySet()))
                        .where(v -> v.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(q -> q.returnVertex())
        );
    }

    @Test
    void returnVertexIdByLabels() {
        assertBuildsStatement("MATCH (v:`a`:`b`) RETURN ID(v)",
                ImmutableMap.of(),
                query()
                        .match(v -> v.labelsMatch(ImmutableSet.of("a", "b")))
                        .andThen(q -> q.returnId())
        );
    }

    @Test
    void updateVertexById() {
        assertBuildsStatement("MATCH (v:`x`) WHERE ID(v)={vertexId1} SET v:`y`:`z` REMOVE v:`x` SET v={vertexProps1}",
                ImmutableMap.of("vertexId1", 1l, "vertexProps1", ImmutableMap.of("a", "c", "e", "f")),
                query()
                        .match(b -> b.labelsMatch(ImmutableSet.of("x")))
                        .where(b -> b.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(b -> b.labels(ImmutableSet.of("x"), ImmutableSet.of("y", "z")))
                        .andThen(b -> b.properties(
                                mockProperties(ImmutableMap.of("a", "b", "u", "v")),
                                mockProperties(ImmutableMap.of("a", "c", "e", "f"))
                        ))
        );
    }


    private VertexQueryBuilder query() {
        return new VertexQueryBuilder(new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.anyLabel());
    }

}