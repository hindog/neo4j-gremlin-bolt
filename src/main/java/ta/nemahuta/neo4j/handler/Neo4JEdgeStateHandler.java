package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.javatuples.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.Relationship;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

public class Neo4JEdgeStateHandler extends AbstractNeo4JElementStateHandler<Neo4JEdgeState> implements RelationProvider {

    private final Neo4JGraphPartition readPartition;

    public Neo4JEdgeStateHandler(@Nonnull final StatementExecutor statementExecutor,
                                 @Nonnull @NonNull final Neo4JGraphPartition readPartition) {
        super(statementExecutor);
        this.readPartition = readPartition;
    }

    @Override
    public Map<String, Set<Long>> loadRelatedIds(final long lhsId,
                                                 @Nonnull @NonNull final Direction direction,
                                                 @Nonnull @NonNull final Set<String> labels) {
        final Map<String, Set<Long>> result = new HashMap<>();
        query()
                .labels(ImmutableSet.copyOf(labels))
                .direction(direction)
                .where(b -> b.getLhs().id(lhsId))
                .andThen(b -> b.returnEdge())
                .build()
                .map(statementExecutor::retrieveRecords)
                .orElseGet(Stream::empty)
                .forEach(r -> addRelation(r, result));
        return result;
    }

    private void addRelation(@Nonnull @NonNull final Record r,
                             @Nonnull @NonNull final Map<String, Set<Long>> result) {
        final Relationship v = r.get(0).asRelationship();
        Optional.ofNullable(result.get(v.type()))
                .orElseGet(() -> {
                    final Set<Long> newEntry = new HashSet<>();
                    result.put(v.type(), newEntry);
                    return newEntry;
                }).add(v.endNodeId());
    }

    @Override
    protected Pair<Long, Neo4JEdgeState> getIdAndConvertToState(final Record r) {
        final Relationship relationship = r.get(0).asRelationship();
        final long inId = relationship.startNodeId();
        final long outId = relationship.endNodeId();
        final Neo4JEdgeState state = new Neo4JEdgeState(relationship.type(), ImmutableMap.copyOf(relationship.asMap()), inId, outId);
        return new Pair<>(relationship.id(), state);
    }

    @Nonnull
    @Override
    protected Statement createDeleteCommand(final long id) {
        return query()
                .direction(Direction.BOTH)
                .labels(Collections.emptySet())
                .where(b -> b.whereId(id))
                .andThen(b -> b.deleteEdge())
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createUpdateCommand(final long id, final Neo4JEdgeState currentState, final Neo4JEdgeState newState) {
        return query()
                .where(b -> b.whereId(id))
                .direction(Direction.BOTH)
                .andThen(b -> b.properties(currentState.getProperties(), newState.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createInsertCommand(@Nonnull final Neo4JEdgeState state) {
        return query()
                .where(b -> b.getLhs().id(state.getOutVertexId()).and(b.getRhs().id(state.getInVertexId())))
                .andThen(b -> b.createEdge(Direction.OUT, state.getLabel(), state.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createLoadCommand(@Nonnull final Set<Long> ids) {
        return query()
                .direction(Direction.OUT)
                .where(b -> b.whereIds(ImmutableSet.copyOf(ids)))
                .andThen(b -> b.returnEdge())
                .build().get();
    }

    /**
     * @return a new {@link VertexQueryBuilder}
     */
    @Nonnull
    protected EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(readPartition);
    }


}
