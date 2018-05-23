package ta.nemahuta.neo4j.query.predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

class WhereIdInPredicateTest extends AbstractStatementBuilderTest {

    @Test
    void idsPresent() {
        assertBuildsStatement("ID(x) IN {ids}", ImmutableMap.of("ids", ImmutableSet.of(1l, 2l, 3l)),
                new WhereIdInPredicate(ImmutableSet.of(1l, 2l, 3l), "x", "ids"));
    }

}