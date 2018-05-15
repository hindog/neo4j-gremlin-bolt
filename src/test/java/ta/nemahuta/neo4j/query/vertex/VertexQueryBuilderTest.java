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

class VertexQueryBuilderTest extends AbstractStatementBuilderTest {

    @Test
    void buildWhereIdAndLabelsReturnVertex() {
        assertBuildsStatement("MATCH (v:`x`:`y`) WHERE v.id={vertexId1} RETURN v",
                ImmutableMap.of("vertexId1", 1l),
                query()
                        .match(q -> q.labelsMatch(ImmutableSet.of("x", "y")))
                        .where(q -> q.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(q -> q.returnVertex())
        );
    }

    @Test
    void createVertexAndReturnId() {
        assertBuildsStatement("CREATE (v:`x`:`y`={vertexProps1}) RETURN v.id",
                ImmutableMap.of("vertexProps1", ImmutableMap.of("a", "b")),
                query()
                        .andThen(q -> q.create(new Neo4JTransientElementId<>(2l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", prop("b"))))
        );
    }

    @Test
    void createVertexAndUseId() {
        assertBuildsStatement("CREATE (v:`x`:`y`={vertexProps1})",
                ImmutableMap.of("vertexProps1", ImmutableMap.of("a", "b", "id", 2l)),
                query()
                        .andThen(q -> q.create(new Neo4JPersistentElementId<>(2l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", prop("b"))))
        );
    }

    @Test
    void deleteVertexById() {
        assertBuildsStatement("MATCH (v) WHERE v.id={vertexId1} DETACH DELETE v",
                ImmutableMap.of("vertexId1", 1l),
                query()
                        .match(v -> v.labelsMatch(Collections.emptySet()))
                        .where(v -> v.id(new Neo4JPersistentElementId<>(1l)))
                        .andThen(q -> q.delete())
        );
    }


    private VertexQueryBuilder query() {
        return new VertexQueryBuilder(new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.anyLabel());
    }

}