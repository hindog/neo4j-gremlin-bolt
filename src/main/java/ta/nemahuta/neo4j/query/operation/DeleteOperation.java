package ta.nemahuta.neo4j.query.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * {@link VertexOperation} which deletes a {@link ta.nemahuta.neo4j.structure.Neo4JVertex}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class DeleteOperation implements VertexOperation, EdgeOperation {

    /**
     * the alias of the deleted element from the MATCH clause
     */
    @NonNull
    private final String alias;

    @Override
    public void append(@Nonnull @NonNull final StringBuilder queryBuilder,
                       @Nonnull @NonNull final Map<String, Object> parameters) {
        queryBuilder.append("DETACH DELETE ").append(alias);
    }

    @Override
    public boolean isNeedsStatement() {
        return true;
    }
}
