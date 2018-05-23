package ta.nemahuta.neo4j.query.edge;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.UniqueParamNameGenerator;
import ta.nemahuta.neo4j.query.WherePredicate;
import ta.nemahuta.neo4j.query.edge.operation.CreateEdgeOperation;
import ta.nemahuta.neo4j.query.edge.operation.ReturnEdgeOperation;
import ta.nemahuta.neo4j.query.operation.DeleteOperation;
import ta.nemahuta.neo4j.query.operation.ReturnIdOperation;
import ta.nemahuta.neo4j.query.operation.UpdatePropertiesOperation;
import ta.nemahuta.neo4j.query.predicate.WhereIdInPredicate;
import ta.nemahuta.neo4j.query.vertex.VertexQueryFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Abstract factory for parameters being used in the {@link EdgeQueryBuilder}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public abstract class EdgeQueryFactory {

    /**
     * @return the alias of the lhs node.
     */
    @Nonnull
    protected abstract String getLhsAlias();

    /**
     * @return the alias of the relation
     */
    @Nonnull
    protected abstract String getRelationAlias();

    /**
     * @return the alias of the rhs node
     */
    @Nonnull
    protected abstract String getRhsAlias();

    /**
     * @return the partition to be used
     */
    @Nonnull
    protected abstract Neo4JGraphPartition getPartition();


    @Getter(onMethod = @__(@Nonnull))
    private final VertexQueryFactory lhs = new VertexQueryFactory() {

        @Nonnull
        @Override
        protected String getAlias() {
            return EdgeQueryFactory.this.getLhsAlias();
        }

        @Override
        @Nonnull
        protected Neo4JGraphPartition getPartition() {
            return EdgeQueryFactory.this.getPartition();
        }

        @Nonnull
        @Override
        protected UniqueParamNameGenerator getParamNameGenerator() {
            return EdgeQueryFactory.this.getParamNameGenerator();
        }
    };

    @Getter(onMethod = @__(@Nonnull))
    private final VertexQueryFactory rhs = new VertexQueryFactory() {

        @Override
        @Nonnull
        protected Neo4JGraphPartition getPartition() {
            return EdgeQueryFactory.this.getPartition();
        }

        @Nonnull
        @Override
        protected String getAlias() {
            return EdgeQueryFactory.this.getRhsAlias();
        }

        @Nonnull
        @Override
        protected UniqueParamNameGenerator getParamNameGenerator() {
            return EdgeQueryFactory.this.getParamNameGenerator();
        }
    };

    /**
     * Construct a predicate which matches the identifier on a relation in a WHERE clause.
     *
     * @param id the identifier to be matched
     * @return the predicate matching the id
     */
    @Nonnull
    public WherePredicate whereId(@Nonnull @NonNull final Long id) {
        return whereIds(Collections.singleton(id));
    }

    /**
     * Construct a predicate which matches the identifier on any of the given ones in a WHERE clause.
     *
     * @param ids the identifiers to match
     * @return the predicate matching any of the ids
     */
    @Nonnull
    public WherePredicate whereIds(@Nonnull @NonNull final Set<Long> ids) {
        return ids.isEmpty() ? WherePredicate.EMPTY :
                new WhereIdInPredicate(ids, getRelationAlias(), getParamNameGenerator().generate("edgeId"));
    }

    /**
     * @return an {@link EdgeOperation} which returns the ids of the nodes and the edge itself
     */
    @Nonnull
    public EdgeOperation returnEdge() {
        return new ReturnEdgeOperation(getLhsAlias(), getRelationAlias(), getRhsAlias());
    }

    /**
     * @return an {@link EdgeOperation} which returns the id of the edge
     */
    @Nonnull
    public EdgeOperation returnId() {
        return new ReturnIdOperation(getRelationAlias());
    }

    /**
     * Create an operation which will create an edge.
     *
     * @param direction  the direction of the edge
     * @param label      the label of the edge
     * @param properties the properties of the edge
     * @return the {@link EdgeOperation} which creates the edge
     */
    @Nonnull
    public EdgeOperation createEdge(@Nonnull @NonNull final Direction direction,
                                    @Nonnull @NonNull final String label,
                                    @Nonnull @NonNull final ImmutableMap<String, Object> properties) {
        return new CreateEdgeOperation(getLhsAlias(), getRelationAlias(), getRhsAlias(), label, direction, properties,
                getParamNameGenerator().generate("edgeProps"));
    }

    /**
     * @return an {@link EdgeOperation} which deletes all edges matching the query
     */
    @Nonnull
    public EdgeOperation deleteEdge() {
        return new DeleteOperation(getRelationAlias());
    }

    /**
     * Create an update {@link EdgeOperation} for changing the properties.
     *
     * @param committedProperties the committed properties
     * @param currentProperties   the current properties (to be changed to)
     * @return the operation
     */
    @Nonnull
    public EdgeOperation properties(@Nonnull @NonNull final ImmutableMap<String, Object> committedProperties,
                                    @Nonnull @NonNull final ImmutableMap<String, Object> currentProperties) {
        return new UpdatePropertiesOperation(committedProperties, currentProperties, getRelationAlias(), getParamNameGenerator().generate("edgeProps"));
    }

    /**
     * @return the parameter name generator
     */
    @Nonnull
    protected abstract UniqueParamNameGenerator getParamNameGenerator();

}
