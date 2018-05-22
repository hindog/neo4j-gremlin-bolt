package ta.nemahuta.neo4j.query.edge.operation;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnEdgeOperationTest extends AbstractStatementBuilderTest {

    private final ReturnEdgeOperation sut = new ReturnEdgeOperation("n", "r", "m");

    @Test
    void isNeedsStatement() {
        assertTrue(sut.isNeedsStatement());
    }

    @Test
    void append() {
        assertBuildsStatement("RETURN r", ImmutableMap.of(), sut);
    }
}