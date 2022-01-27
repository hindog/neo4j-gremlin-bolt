package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Relationship;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.edge.EdgeQueryFactory;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class Neo4JEdgeStateHandler extends AbstractNeo4JElementStateHandler<Neo4JEdgeState, EdgeQueryBuilder> {

    private final Neo4JGraphPartition readPartition;

    public Neo4JEdgeStateHandler(@Nonnull final StatementExecutor statementExecutor,
                                 @Nonnull final Neo4JGraphPartition readPartition) {
        super(statementExecutor);
        this.readPartition = readPartition;
    }


    @Nonnull
    @Override
    protected Neo4JEdgeState convertToState(@Nonnull final Record r) {
        final Relationship relationship = r.get(0).asRelationship();
        final long inId = relationship.endNodeId();
        final long outId = relationship.startNodeId();
        return new Neo4JEdgeState(relationship.type(), ImmutableMap.copyOf(relationship.asMap()), inId, outId);
    }

    @Nonnull
    @Override
    protected Long getId(@Nonnull final Record r) {
        final Relationship relationship = r.get(0).asRelationship();
        return relationship.id();
    }

    @Nonnull
    @Override
    protected Query createDeleteCommand(final long id) {
        return query()
                .direction(Direction.BOTH)
                .labels(Collections.emptySet())
                .where(b -> b.whereId(id))
                .andThen(EdgeQueryFactory::deleteEdge)
                .build().get();
    }

    @Nonnull
    @Override
    protected Query createUpdateCommand(final long id, final Neo4JEdgeState currentState, final Neo4JEdgeState newState) {
        return query()
                .where(b -> b.whereId(id))
                .direction(Direction.BOTH)
                .andThen(b -> b.properties(currentState.getProperties(), newState.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Query createInsertCommand(@Nonnull final Neo4JEdgeState state) {
        return query()
                .where(b -> b.getLhs().id(state.getOutVertexId()).and(b.getRhs().id(state.getInVertexId())))
                .andThen(b -> b.createEdge(Direction.OUT, state.getLabel(), state.getProperties()))
                .build().get();
    }

    @Nonnull
    @Override
    protected Query createLoadCommand(@Nonnull final Set<Long> ids) {
        return query()
                .direction(Direction.OUT)
                .where(b -> b.whereIds(ImmutableSet.copyOf(ids)))
                .andThen(b -> b.returnEdge())
                .build().get();
    }

    @Nonnull
    @Override
    protected Query createLoadAllIdsCommand() {
        return query()
                .direction(Direction.BOTH)
                .andThen(EdgeQueryFactory::returnId).build().get();
    }

    @Nonnull
    @Override
    protected Query createCreateIndexCommand(@Nonnull final String label,
                                             @Nonnull final Set<String> propertyNames) {
        return query()
                .andThen(b -> b.createPropertyIndex(label, propertyNames))
                .build().get();
    }

    /**
     * @return a new {@link VertexQueryBuilder}
     */
    @Override
    protected EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(readPartition);
    }

}
