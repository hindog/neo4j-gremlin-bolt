package ta.nemahuta.neo4j.query.operation;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.QueryUtils;
import ta.nemahuta.neo4j.query.edge.EdgeOperation;
import ta.nemahuta.neo4j.query.vertex.VertexOperation;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CreatePropertyIndex implements VertexOperation, EdgeOperation {

    @NonNull
    private final String label;

    @NonNull
    private final Set<String> propertyNames;

    @Override
    public boolean isNeedsStatement() {
        return true;
    }

    @Override
    public void append(@Nonnull final StringBuilder queryBuilder, @Nonnull final Map<String, Object> parameters) {
        queryBuilder.replace(0, queryBuilder.length(), "CREATE INDEX ON ");
        QueryUtils.appendLabels(queryBuilder, Collections.singleton(label));
        queryBuilder.append("(").append(propertyNames.stream().collect(Collectors.joining(","))).append(")");
    }
}
