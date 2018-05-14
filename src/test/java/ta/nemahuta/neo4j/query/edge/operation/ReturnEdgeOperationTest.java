package ta.nemahuta.neo4j.query.edge.operation;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnEdgeOperationTest extends AbstractStatementBuilderTest {


    private final ReturnEdgeOperation sut = new ReturnEdgeOperation("n", "r", "m", new Neo4JNativeElementIdAdapter());

    @Test
    void isNeedsStatement() {
        assertTrue(sut.isNeedsStatement());
    }

    @Test
    void append() {
        assertBuildsStatement("RETURN n.id, r, m.id", ImmutableMap.of(), sut);
    }
}