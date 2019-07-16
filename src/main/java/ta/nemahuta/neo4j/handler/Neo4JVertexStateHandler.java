package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.Node;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryFactory;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class Neo4JVertexStateHandler extends AbstractNeo4JElementStateHandler<Neo4JVertexState, VertexQueryBuilder> {

    private final Neo4JGraphPartition readPartition;

    public Neo4JVertexStateHandler(@Nonnull final StatementExecutor statementExecutor,
                                   @Nonnull final Neo4JGraphPartition readPartition) {
        super(statementExecutor);
        this.readPartition = readPartition;
    }

    @Nonnull
    @Override
    protected Neo4JVertexState convertToState(@Nonnull final Record r) {
        final Node n = r.get(0).asNode();
        return new Neo4JVertexState(
                ImmutableSet.copyOf(readPartition.ensurePartitionLabelsNotSet(n.labels())),
                ImmutableMap.copyOf(n.asMap()));
    }

    @Nonnull
    @Override
    protected Long getId(@Nonnull final Record r) {
        final Node n = r.get(0).asNode();
        return n.id();
    }

    @Nonnull
    @Override
    protected Statement createDeleteCommand(final long id) {
        return query()
                .match(b -> b.labelsMatch(Collections.emptySet()))
                .where(b -> b.id(id))
                .andThen(VertexQueryFactory::delete)
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createUpdateCommand(final long id, final Neo4JVertexState currentState, final Neo4JVertexState newState) {
        return query()
                .match(b -> b.labelsMatch(Collections.emptySet()))
                .where(b -> b.id(id))
                .andThen(b -> b.labels(currentState.getLabels(), newState.getLabels()))
                .andThen(b -> b.properties(currentState.getProperties(), newState.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createInsertCommand(@Nonnull final Neo4JVertexState state) {
        return query()
                .andThen(b -> b.create(state.getLabels(), state.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createLoadCommand(@Nonnull final Set<Long> ids) {
        return query()
                .match(b -> b.labelsMatch(readPartition.ensurePartitionLabelsSet(Collections.emptySet())))
                .where(b -> b.idsInSet(ids))
                .andThen(VertexQueryFactory::returnVertex)
                .build().get();
    }

    @Nonnull
    @Override
    protected Statement createLoadAllIdsCommand() {
        return query()
                .match(b -> b.labelsMatch(readPartition.ensurePartitionLabelsSet(Collections.emptySet())))
                .andThen(VertexQueryFactory::returnId).build().get();
    }

    @Nonnull
    @Override
    protected Statement createCreateIndexCommand(@Nonnull final String label,
                                                 @Nonnull final Set<String> propertyNames) {
        return query()
                .andThen(b -> b.createPropertyIndex(label, propertyNames))
                .build().get();
    }

    @Override
    protected VertexQueryBuilder query() {
        return new VertexQueryBuilder(readPartition);
    }
}
