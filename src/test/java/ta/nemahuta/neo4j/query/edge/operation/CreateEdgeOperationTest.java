package ta.nemahuta.neo4j.query.edge.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateEdgeOperationTest extends AbstractStatementBuilderTest {

    private final CreateEdgeOperation sut = new CreateEdgeOperation("n", "r", "m",
            "test", Direction.IN, ImmutableMap.of("golden", "retriever"),
            "props");

    @Test
    void isNeedsStatement() {
        assertTrue(sut.isNeedsStatement());
    }

    @Test
    void append() {
        assertBuildsStatement("CREATE (n)<-[r:`test`]-(m) SET r={props} RETURN ID(r)",
                ImmutableMap.of("props", ImmutableMap.of("golden", "retriever")),
                sut
        );
    }
}