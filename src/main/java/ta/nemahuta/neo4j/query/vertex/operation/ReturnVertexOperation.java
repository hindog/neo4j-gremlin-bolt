package ta.nemahuta.neo4j.query.vertex.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import java.util.Map;

/**
 * {@link VertexOperation} which returns the complete vertex.
 */
@RequiredArgsConstructor
public class ReturnVertexOperation implements VertexOperation {

    /**
     * the alias of the vertex from the MATCH clause
     */
    @NonNull
    private final String alias;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(final StringBuilder queryBuilder, final Map<String, Object> parameters) {
        queryBuilder.append("RETURN ").append(alias);
    }

}
