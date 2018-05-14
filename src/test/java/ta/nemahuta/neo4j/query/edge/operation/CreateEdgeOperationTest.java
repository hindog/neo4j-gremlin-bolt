package ta.nemahuta.neo4j.query.edge.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;
import ta.nemahuta.neo4j.state.PropertyCardinality;
import ta.nemahuta.neo4j.state.PropertyValue;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateEdgeOperationTest extends AbstractStatementBuilderTest {


    @Nonnull
    private CreateEdgeOperation createOperation(final Neo4JElementId<?> id) {
        return new CreateEdgeOperation("n", "r", "m",
                id, "test", Direction.IN, ImmutableMap.of("x", PropertyValue.from(1l, "y", PropertyCardinality.SINGLE)),
                "props", new Neo4JNativeElementIdAdapter());
    }

    @Test
    void isNeedsStatement() {
        assertTrue(createOperation(new Neo4JTransientElementId<>(1l)).isNeedsStatement());
    }

    @Test
    void append() {
        assertBuildsStatement("CREATE (n)<-[r:`test`={props}]-(m) RETURN r.id",
                ImmutableMap.of("props", ImmutableMap.of("x", "y")),
                createOperation(new Neo4JTransientElementId<>(1l))
        );
        assertBuildsStatement("CREATE (n)<-[r:`test`={props}]-(m)",
                ImmutableMap.of("props", ImmutableMap.of("x", "y"), "id", 1l),
                createOperation(new Neo4JPersistentElementId<>(1l)));
    }
}