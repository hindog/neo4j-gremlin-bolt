package ta.nemahuta.neo4j.query.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link VertexOperation} which updates the properties of a {@link ta.nemahuta.neo4j.structure.Neo4JVertex}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class UpdatePropertiesOperation implements VertexOperation, EdgeOperation {

    /**
     * the currently committed properties
     */
    @NonNull
    private final Map<String, Object> committedProperties;
    /**
     * the current properties to be committed
     */
    @NonNull
    private final Map<String, Object> currentProperties;
    /**
     * the alias of the MATCH the properties should be set on
     */
    @NonNull
    private final String alias;
    /**
     * the parameter name
     */
    @NonNull
    private final String paramName;

    @Override
    public boolean isNeedsStatement() {
        return !committedProperties.equals(currentProperties);
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder,
                       @Nonnull final Map<String, Object> parameters) {
        queryBuilder.append("SET ").append(alias).append("={").append(paramName).append("}");
        parameters.put(paramName, currentProperties);
    }
}
