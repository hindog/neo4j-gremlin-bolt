package ta.nemahuta.neo4j.query.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link EdgeOperation} and {@link VertexOperation} which returns the identifier.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class ReturnIdOperation implements EdgeOperation, VertexOperation {

    /**
     * the alias of the MATCH clause to return the id from
     */
    @NonNull
    private final String alias;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder,
                       @Nonnull final Map<String, Object> parameters) {
        queryBuilder.append("RETURN ID(").append(alias).append(")");
    }
}
