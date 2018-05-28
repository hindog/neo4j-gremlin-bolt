package ta.nemahuta.neo4j.query;

import javax.annotation.Nonnull;

/**
 * Marker interface for a {@link QueryPredicate} to be used in a WHERE clause.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface WherePredicate extends QueryPredicate {

    WherePredicate EMPTY = (queryBuilder, parameters) -> {
    };

    /**
     * Joins this predicate and another one with an AND operator.
     *
     * @param rhs the right hand side to join
     * @return the new predicate
     */
    @Nonnull
    default WherePredicate and(@Nonnull final WherePredicate rhs) {
        if (this == EMPTY) {
            return rhs;
        } else if (rhs == EMPTY) {
            return this;
        }
        final QueryPredicate lhs = this;
        return (queryBuilder, parameters) -> {
            lhs.append(queryBuilder, parameters);
            queryBuilder.append(" AND ");
            rhs.append(queryBuilder, parameters);
        };
    }

    /**
     * Joins this predicate and another one with an OR operator.
     *
     * @param rhs the right hand side to join
     * @return the new predicate
     */
    @Nonnull
    default WherePredicate or(@Nonnull final WherePredicate rhs) {
        if (this == EMPTY) {
            return rhs;
        } else if (rhs == EMPTY) {
            return this;
        }
        final QueryPredicate lhs = this;
        return (queryBuilder, parameters) -> {
            lhs.append(queryBuilder, parameters);
            queryBuilder.append(" OR ");
            rhs.append(queryBuilder, parameters);
        };
    }

    /**
     * Wraps this predicate in brackets.
     *
     * @return the new predicate
     */
    @Nonnull
    default WherePredicate inBrackets() {
        final QueryPredicate inner = this;
        return (queryBuilder, parameters) -> {
            queryBuilder.append("(");
            inner.append(queryBuilder, parameters);
            queryBuilder.append(")");
        };
    }

    @Nonnull
    static WherePredicate orOp(@Nonnull final WherePredicate w1,
                               @Nonnull final WherePredicate w2) {
        return w1.or(w2);
    }

}
