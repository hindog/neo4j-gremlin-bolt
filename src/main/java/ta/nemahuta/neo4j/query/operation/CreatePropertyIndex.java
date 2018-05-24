package ta.nemahuta.neo4j.query.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class CreatePropertyIndex implements VertexOperation, EdgeOperation {

    @NonNull
    private final Set<String> labels;

    @NonNull
    private final String propertyName;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder, @Nonnull final Map<String, Object> parameters) {
        queryBuilder.replace(0, queryBuilder.length(), "CREATE INDEX ON ");
        QueryUtils.appendLabels(queryBuilder, labels);
        queryBuilder.append("(").append(propertyName).append(")");
    }
}
