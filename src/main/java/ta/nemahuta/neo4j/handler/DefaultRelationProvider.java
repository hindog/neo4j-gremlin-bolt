package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.Relationship;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.session.StatementExecutor;

import javax.annotation.Nonnull;
import java.util.*;

@RequiredArgsConstructor
public class DefaultRelationProvider implements RelationProvider {

    @NonNull
    private final StatementExecutor statementExecutor;

    private final Neo4JGraphPartition readPartition;

    @Override
    public Map<String, Set<Long>> loadRelatedIds(final long lhsId, @Nonnull final Direction direction, @Nonnull final Set<String> labels) {
        final Map<String, Set<Long>> result = new HashMap<>();
        statementExecutor.retrieveRecords(createRelatedIdStatement(lhsId, direction, labels)).forEach(r -> addToResult(r, result));
        return result;

    }

    private void addToResult(final Record r, final Map<String, Set<Long>> result) {
        final Relationship v = r.get(0).asRelationship();
        Optional.ofNullable(result.get(v.type()))
                .orElseGet(() -> {
                    final Set<Long> newEntry = new HashSet<>();
                    result.put(v.type(), newEntry);
                    return newEntry;
                }).add(v.endNodeId());
    }

    protected Statement createRelatedIdStatement(final long lhsId, final Direction direction, final Set<String> labels) {
        return query()
                .labels(ImmutableSet.copyOf(labels))
                .direction(direction)
                .where(b -> b.getLhs().id(lhsId))
                .andThen(b -> b.returnEdge())
                .build().get();
    }

    /**
     * @return a new {@link VertexQueryBuilder
     */
    @Nonnull
    protected EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(readPartition);
    }
}
