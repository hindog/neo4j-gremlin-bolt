package ta.nemahuta.neo4j.partition;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Neo4JLabelGraphPartitionTest {

    @Nested
    class AllLabelsOf {

        private final Neo4JGraphPartition partition = Neo4JLabelGraphPartition.allLabelsOf("yeah", "no");

        @Test
        void ensurePartitionLabelsSet() {
            // when: 'ensuring all labels are set'
            final Set<String> actual = partition.ensurePartitionLabelsSet(ImmutableSet.of("huh"));
            // then: 'one of the labels is set'
            assertEquals(ImmutableSet.of("huh", "yeah", "no"), actual);
        }

        @Test
        void ensurePartitionLabelsNotSet() {
            // when: 'ensuring all labels are set'
            final Set<String> actual = partition.ensurePartitionLabelsNotSet(ImmutableSet.of("huh", "yeah", "no"));
            // then: 'none of the labels is set'
            assertEquals(ImmutableSet.of("huh"), actual);
        }

        @Test
        void vertexWhereLabelPredicate() {
            // when: 'requesting the label predicate'
            final Optional<WherePredicate> predicate = partition.vertexWhereLabelPredicate("v");
            // then: 'the predicate is correct'
            assertEquals("v:`yeah`:`no`", renderPredicate(predicate));
        }

    }

    @Nested
    class AnyLabel {

        private final Neo4JGraphPartition partition = Neo4JLabelGraphPartition.anyLabel();

        @Test
        void ensurePartitionLabelsSet() {
            // when: 'ensuring all labels are set'
            final Set<String> actual = partition.ensurePartitionLabelsSet(ImmutableSet.of("huh"));
            // then: 'one of the labels is set'
            assertEquals(ImmutableSet.of("huh"), actual);
        }

        @Test
        void ensurePartitionLabelsNotSet() {
            // when: 'ensuring all labels are set'
            final Set<String> actual = partition.ensurePartitionLabelsNotSet(ImmutableSet.of("huh", "yeah", "no"));
            // then: 'none of the labels is set'
            assertEquals(ImmutableSet.of("huh", "yeah", "no"), actual);
        }

        @Test
        void vertexWhereLabelPredicate() {
            // when: 'requesting the label predicate'
            final Optional<WherePredicate> predicate = partition.vertexWhereLabelPredicate("v");
            // then: 'the predicate is correct'
            assertEquals("", renderPredicate(predicate));
        }

    }

    private static String renderPredicate(@Nonnull final Optional<WherePredicate> wherePredicateOptional) {
        final StringBuilder sb = new StringBuilder();
        final Map<String, Object> parameters = new HashMap<>();
        wherePredicateOptional.ifPresent(p -> p.append(sb, parameters));
        return sb.toString();
    }

}