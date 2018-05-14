package ta.nemahuta.neo4j.query.edge.predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.query.AbstractStatementBuilderTest;

class MatchRelationPredicateTest extends AbstractStatementBuilderTest {

    private MatchRelationPredicate createMatchRelationPredicate() {
        return new MatchRelationPredicate("n", "r", "m");
    }

    @Test
    void appendUndefinedLhsRhs() {
        final MatchRelationPredicate sut = createMatchRelationPredicate();
        sut.setLabels(ImmutableSet.of("a"));
        sut.setDirection(Direction.IN);
        assertBuildsStatement("n<-[r:`a`]-m", ImmutableMap.of(), sut);
    }

}