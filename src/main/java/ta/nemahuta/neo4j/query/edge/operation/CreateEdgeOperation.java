package ta.nemahuta.neo4j.query.edge.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;

/**
 * Operation which creates an edge between two nodes and sets the label and properties for it.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class CreateEdgeOperation implements EdgeOperation {

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
     * the label for the relation
     */
    @NonNull
    private final String label;
    /**
     * the direction for the relation
     */
    @NonNull
    private final Direction direction;
    /**
     * the properties to be set
     */
    @NonNull
    private final Map<String, Object> properties;
    /**
     * the parameter name for the properties
     */
    @NonNull
    private final String paramProperties;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder,
                       @Nonnull final Map<String, Object> parameters) {
        queryBuilder.append("CREATE (").append(lhsAlias).append(")");
        QueryUtils.appendRelationStart(direction, queryBuilder);
        queryBuilder.append(relationAlias);
        QueryUtils.appendLabels(queryBuilder, Collections.singleton(label));
        QueryUtils.appendRelationEnd(direction, queryBuilder);
        queryBuilder.append("(").append(rhsAlias).append(")");
        queryBuilder.append(" SET ").append(relationAlias).append("={").append(paramProperties).append("}");
        queryBuilder.append(" RETURN ").append("ID(").append(relationAlias).append(")");
        parameters.put(paramProperties, properties);
    }
}
