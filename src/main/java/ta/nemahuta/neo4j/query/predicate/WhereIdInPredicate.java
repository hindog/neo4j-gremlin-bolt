package ta.nemahuta.neo4j.query.predicate;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link WherePredicate} which matches an {@link Neo4JElementId} to a {@link Set} of them a {@link Neo4JGraphPartition}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class WhereIdInPredicate implements WherePredicate {

    /**
     * the {@link Neo4JElementIdAdapter} to be used to determine the property name holding the identifier for the element
     */
    @NonNull
    private final Neo4JElementIdAdapter<?> idAdapter;
    /**
     * the ids to be matched (one of them is enough)
     */
    @NonNull
    private final Set<Neo4JElementId<?>> ids;
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
        if (ids.size() == 1) {
            // In case we have a single id, we use the single statement
            queryBuilder.append(alias).append(".").append(idAdapter.propertyName()).append(" = {").append(paramName).append("}");
            parameters.put(paramName, ids.iterator().next().getId());
        } else {
            queryBuilder.append(alias).append(".").append(idAdapter.propertyName()).append(" IN {").append(paramName).append("}");
            parameters.put(paramName, ids.stream().map(Neo4JElementId::getId).collect(Collectors.toList()));
        }
    }

}
