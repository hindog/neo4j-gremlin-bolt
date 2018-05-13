package ta.nemahuta.neo4j.query.edge.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
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
    /**
     * the id adapter for the vertexes
     */
    @NonNull
    private final Neo4JElementIdAdapter<?> vertexIdAdapter;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull @NonNull final StringBuilder queryBuilder,
                       @Nonnull @NonNull final Map<String, Object> parameters) {
        queryBuilder.append("RETURN ")
                .append(lhsAlias).append(".").append(vertexIdAdapter.propertyName())
                .append(", ")
                .append(relationAlias)
                .append(", ")
                .append(rhsAlias).append(".").append(vertexIdAdapter.propertyName());
    }

}
