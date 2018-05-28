package ta.nemahuta.neo4j.query.operation;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link VertexOperation} which updates orLabelsAnd for a {@link ta.nemahuta.neo4j.structure.Neo4JVertex}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class UpdateLabelsOperation implements VertexOperation, EdgeOperation {

    /**
     * the committed state of the labels
     */
    @NonNull
    private final Set<String> committedLabels;
    /**
     * the actual state of the labels to be committed
     */
    @NonNull
    private final Set<String> currentLabels;
    /**
     * the alias of the element from the MATCH clause to set the labels on
     */
    @NonNull
    private final String alias;


    @Override
    public void append(@Nonnull final StringBuilder queryBuilder,
                       @Nonnull final Map<String, Object> parameters) {
        final int idx = queryBuilder.length();
        ImmutableMap.of(
                "SET", getAddedLabels().collect(Collectors.toSet()),
                "REMOVE", getRemovedLabels().collect(Collectors.toSet())
        ).forEach((op, set) -> {
            if (!set.isEmpty()) {
                if (idx < queryBuilder.length()) {
                    queryBuilder.append(" ");
                }
                queryBuilder.append(op).append(" ").append(alias);
                QueryUtils.appendLabels(queryBuilder, set);
            }
        });
    }

    @Override
    public boolean isNeedsStatement() {
        return getAddedLabels().findAny().isPresent() || getRemovedLabels().findAny().isPresent();
    }

    /**
     * @return the {@link Stream} of all orLabelsAnd to be added
     */
    @Nonnull
    private Stream<String> getAddedLabels() {
        return currentLabels.stream().filter(l -> !committedLabels.contains(l));
    }

    /**
     * @return the {@link Stream} of all orLabelsAnd to be removed
     */
    @Nonnull
    private Stream<String> getRemovedLabels() {
        return committedLabels.stream().filter(l -> !currentLabels.contains(l));
    }

}
