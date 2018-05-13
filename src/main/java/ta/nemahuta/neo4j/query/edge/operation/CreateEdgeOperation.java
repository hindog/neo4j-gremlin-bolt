package ta.nemahuta.neo4j.query.edge.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.state.PropertyValue;

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
     * the identifier for the relation
     */
    @NonNull
    private final Neo4JElementId<?> id;
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
    private final Map<String, PropertyValue<?>> properties;
    /**
     * the parameter name for the properties
     */
    @NonNull
    private final String paramProperties;
    /**
     * the identifier adapter for the relation
     */
    @NonNull
    private final Neo4JElementIdAdapter<?> idAdapter;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@NonNull @Nonnull final StringBuilder queryBuilder,
                       @NonNull @Nonnull final Map<String, Object> parameters) {
        queryBuilder.append("CREATE (").append(lhsAlias).append(")");

        QueryUtils.appendRelationStart(direction, queryBuilder);
        queryBuilder.append(relationAlias);
        QueryUtils.appendLabels(queryBuilder, Collections.singleton(label));
        queryBuilder.append("= {").append(paramProperties).append("}");
        QueryUtils.appendRelationEnd(direction, queryBuilder);

        queryBuilder.append("(").append(rhsAlias).append(")");

        final Map<String, Object> properties = QueryUtils.computeProperties(Collections.emptyMap(), this.properties);
        if (id.isRemote()) {
            properties.put(idAdapter.propertyName(), id.getId());
        } else {
            queryBuilder.append("RETURN ").append(relationAlias).append(".").append(idAdapter.propertyName());
        }
        parameters.put(paramProperties, properties);
    }
}
