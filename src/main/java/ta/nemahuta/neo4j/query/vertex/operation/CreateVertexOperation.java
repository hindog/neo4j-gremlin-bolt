package ta.nemahuta.neo4j.query.vertex.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;
import ta.nemahuta.neo4j.state.PropertyValue;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * {@link VertexOperation} which creates a {@link ta.nemahuta.neo4j.structure.Neo4JVertex}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class CreateVertexOperation implements VertexOperation {

    /**
     * the {@link Neo4JElementIdAdapter} being used for the identifiers
     */
    @NonNull
    private final Neo4JElementIdAdapter<?> idAdapter;
    /**
     * the current {@link Neo4JElementId} of the element
     */
    @NonNull
    private final Neo4JElementId<?> id;
    /**
     * the labels for the new vertex
     */
    @NonNull
    private final Set<String> labels;
    /**
     * the properties to be set for the new vertex
     */
    @NonNull
    private final Map<String, PropertyValue<?>> properties;
    /**
     * the alias of the vertex from the MATCH clause
     */
    @NonNull
    private final String alias;
    /**
     * the name of the parameter providing the vertex' properties
     */
    @NonNull
    private final String propertiesParam;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull @NonNull final StringBuilder queryBuilder,
                       @Nonnull @NonNull final Map<String, Object> parameters) {
        queryBuilder.append("CREATE (n");
        QueryUtils.appendLabels(queryBuilder, labels);
        queryBuilder.append("{").append(propertiesParam).append("})");
        final Map<String, Object> properties = QueryUtils.computeProperties(Collections.emptyMap(), this.properties);
        if (!id.isRemote()) {
            properties.put(idAdapter.propertyName(), id.getId());
        }
        parameters.put(propertiesParam, properties);
        if (!id.isRemote()) {
            queryBuilder.append("RETURN ").append(alias).append(".").append(idAdapter.propertyName());
        }
    }

}
