package ta.nemahuta.neo4j.query.vertex.predicate;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.MatchPredicate;
import ta.nemahuta.neo4j.query.QueryUtils;

import java.util.Map;
import java.util.Set;

/**
 * {@link MatchPredicate} for matching labels.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class MatchAllVertexLabelsPredicate implements MatchPredicate {

    /**
     * the labels to be matched
     */
    @NonNull
    private final Set<String> labels;
    /**
     * the alias for the vertex
     */
    @NonNull
    private final String alias;

    @Override
    public void append(final StringBuilder queryBuilder, final Map<String, Object> parameters) {
        queryBuilder.append("(").append(alias);
        QueryUtils.appendLabels(queryBuilder, labels);
        queryBuilder.append(")");
    }
}
