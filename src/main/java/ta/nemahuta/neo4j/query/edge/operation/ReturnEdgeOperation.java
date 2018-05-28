package ta.nemahuta.neo4j.query.edge.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * An operation which returns the node ids and the edge itself.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class ReturnEdgeOperation implements EdgeOperation {

    /**
     * the alias for the lhs node
     */
    @NonNull
    private final String lhsAlias;
    /**
     * the alias for the relation
     */
    @NonNull
    private final String relationAlias;
    /**
     * the alias for the rhs node
     */
    @NonNull
    private final String rhsAlias;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder,
                       @Nonnull final Map<String, Object> parameters) {
        queryBuilder.append("RETURN ").append(relationAlias);
    }

}
