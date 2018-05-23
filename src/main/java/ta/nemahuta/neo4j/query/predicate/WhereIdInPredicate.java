package ta.nemahuta.neo4j.query.predicate;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * {@link WherePredicate} which matches an identifier to a {@link Set} of them a {@link Neo4JGraphPartition}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class WhereIdInPredicate implements WherePredicate {

    /**
     * the ids to be matched (one of them is enough)
     */
    @NonNull
    private final Set<Long> ids;
    /**
     * the alias of the element from the MATCH clause
     */
    @NonNull
    private final String alias;

    /**
     * the parameter name to be used
     */
    @NonNull
    private final String paramName;

    @Override
    public void append(@Nonnull @NonNull final StringBuilder queryBuilder,
                       @Nonnull @NonNull final Map<String, Object> parameters) {
        // First append the match predicate, in case the partition is limited
        queryBuilder.append("ID(").append(alias).append(")").append(" IN {").append(paramName).append("}");
        parameters.put(paramName, ids);
    }

}
