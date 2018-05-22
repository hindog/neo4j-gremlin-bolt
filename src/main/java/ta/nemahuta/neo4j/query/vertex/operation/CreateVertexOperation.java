package ta.nemahuta.neo4j.query.vertex.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
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
     * the labels for the new vertex
     */
    @NonNull
    private final Set<String> labels;
    /**
     * the properties to be set for the new vertex
     */
    @NonNull
    private final Map<String, Object> properties;
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

        queryBuilder.append("CREATE (").append(alias);
        QueryUtils.appendLabels(queryBuilder, labels);
        queryBuilder.append(") SET ").append(alias).append("={").append(propertiesParam).append("}")
                .append(" RETURN ID(").append(alias).append(")");
        parameters.put(propertiesParam, properties);
    }

}
