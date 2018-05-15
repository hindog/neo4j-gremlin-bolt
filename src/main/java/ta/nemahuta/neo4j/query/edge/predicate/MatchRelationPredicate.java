package ta.nemahuta.neo4j.query.edge.predicate;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.query.MatchPredicate;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.vertex.predicate.MatchAllVertexLabelsPredicate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link MatchPredicate} which matches a relation between two nodes.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class MatchRelationPredicate implements MatchPredicate {

    /**
     * the alias for the lhs node
     */
    @NonNull
    private final String lhsAlias;
    /**
     * the alias of the relation ship
     */
    @NonNull
    private final String relationAlias;
    /**
     * the alias for the lhs node
     */
    @NonNull
    private final String rhsAlias;

    /**
     * the {@link MatchPredicate} for the lhs node
     */
    @Setter
    private MatchPredicate lhs;
    /**
     * the {@link MatchPredicate} for the rhs node
     */
    @Setter
    private MatchPredicate rhs;
    /**
     * the direction of the relation
     */
    @Setter
    @Getter
    private Direction direction;
    /**
     * the labels for the match (in terms of one-of-the-labels)
     */
    @Setter
    @Getter
    private Set<String> labels;

    @Override
    public void append(@Nonnull @NonNull final StringBuilder queryBuilder,
                       @Nonnull @NonNull final Map<String, Object> parameters) {

        Optional.ofNullable(lhs)
                .orElseGet(() -> this.defaultLabelMatcher(lhsAlias))
                .append(queryBuilder, parameters);
        if (direction != null) {
            // In case a relation is set to be queried, we append the relation
            appendRelation(queryBuilder);
        } else {
            // Otherwise we match the pure nodes at the end of the edges
            queryBuilder.append(", ");
        }
        Optional.ofNullable(rhs)
                .orElseGet(() -> this.defaultLabelMatcher(rhsAlias))
                .append(queryBuilder, parameters);
    }

    private MatchPredicate defaultLabelMatcher(final String alias) {
        return new MatchAllVertexLabelsPredicate(Collections.emptySet(), alias);
    }

    private void appendRelation(@Nonnull @NonNull final StringBuilder queryBuilder) {
        QueryUtils.appendRelationStart(direction, queryBuilder);
        queryBuilder.append(relationAlias);
        final int idx = queryBuilder.length();
        // Join all relation orLabelsAnd using an OR
        labels.forEach(label -> {
            if (idx < queryBuilder.length()) {
                queryBuilder.append("|");
            }
            QueryUtils.appendLabels(queryBuilder, Collections.singleton(label));
        });
        QueryUtils.appendRelationEnd(direction, queryBuilder);
    }

}
