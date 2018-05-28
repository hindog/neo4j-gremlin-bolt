package ta.nemahuta.neo4j.query.edge;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.query.MatchPredicate;
import ta.nemahuta.neo4j.query.UniqueParamNameGenerator;
import ta.nemahuta.neo4j.query.WherePredicate;
import ta.nemahuta.neo4j.query.edge.predicate.MatchRelationPredicate;
import ta.nemahuta.neo4j.query.vertex.VertexQueryFactory;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An {@link AbstractQueryBuilder} for {@link ta.nemahuta.neo4j.structure.Neo4JEdge}s.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class EdgeQueryBuilder extends AbstractQueryBuilder {

    /**
     * default alias for vertices on the lhs
     */
    public static final String VERTEX_ALIAS_LHS = "n";
    /**
     * default alias for vertices on the rhs
     */
    public static final String VERTEX_ALIAS_RHS = "m";
    /**
     * default alias for relations
     */
    public static final String RELATION_ALIAS = "r";

    private final EdgeQueryFactory factory = new EdgeQueryFactory() {

        @Getter(value = AccessLevel.PROTECTED, onMethod = @__(@Override))
        protected final UniqueParamNameGenerator paramNameGenerator = new UniqueParamNameGenerator();

        @Override
        protected String getLhsAlias() {
            return VERTEX_ALIAS_LHS;
        }

        @Override
        protected String getRelationAlias() {
            return RELATION_ALIAS;
        }

        @Override
        protected String getRhsAlias() {
            return VERTEX_ALIAS_RHS;
        }

        @Nonnull
        @Override
        protected Neo4JGraphPartition getPartition() {
            return EdgeQueryBuilder.this.partition;
        }
    };

    private final MatchRelationPredicate relationPredicate = new MatchRelationPredicate(VERTEX_ALIAS_LHS, RELATION_ALIAS, VERTEX_ALIAS_RHS, partition);

    /**
     * Create a new builder using the provided parameters.
     *
     * @param partition the {@link Neo4JGraphPartition} being operated on
     */
    public EdgeQueryBuilder(@Nonnull final Neo4JGraphPartition partition) {
        super(partition);
        setMatch(relationPredicate);
    }

    /**
     * Add a {@link MatchPredicate} to the lhs node.
     *
     * @param lhsMatchBuilder the builder to be used to construct the predicate
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder lhsMatch(@Nonnull final Function<VertexQueryFactory, MatchPredicate> lhsMatchBuilder) {
        return match(factory.getLhs(), relationPredicate::setLhs, lhsMatchBuilder);
    }

    /**
     * Add a {@link MatchPredicate} to the rhs node.
     *
     * @param rhsMatchBuilder the builder to be used to construct the predicate
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder rhsMatch(@Nonnull final Function<VertexQueryFactory, MatchPredicate> rhsMatchBuilder) {
        return match(factory.getRhs(), relationPredicate::setRhs, rhsMatchBuilder);
    }

    /**
     * Set the {@link Direction} for the query.
     *
     * @param direction the direction
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder direction(@Nonnull final Direction direction) {
        this.relationPredicate.setDirection(direction);
        return this;
    }

    /**
     * Set the labels for the relation of the query.
     *
     * @param labels the labels to be used
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder labels(@Nonnull final Set<String> labels) {
        this.relationPredicate.setLabels(labels);
        return this;
    }

    /**
     * Set the parameters for the WHERE clause.
     *
     * @param whereBuilder the builder to be used to construct them
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder where(@Nonnull final Function<EdgeQueryFactory, WherePredicate> whereBuilder) {
        setWhere(whereBuilder.apply(factory));
        return this;
    }

    /**
     * Add an operation to be executed with the query.
     *
     * @param opBuilder the builder which creates the operation
     * @return {@code this}
     */
    @Nonnull
    public EdgeQueryBuilder andThen(@Nonnull final Function<EdgeQueryFactory, EdgeOperation> opBuilder) {
        addOperation(opBuilder.apply(factory));
        return this;
    }

    /**
     * Add a MATCH clause to the query.
     *
     * @param queryFactory the query factory
     * @param setter       the setter for the part of the MATCH clause
     * @param matchBuilder the builder to be invoked
     * @return {@code this}
     */
    @Nonnull
    private EdgeQueryBuilder match(@Nonnull final VertexQueryFactory queryFactory,
                                   @Nonnull final Consumer<MatchPredicate> setter,
                                   @Nonnull final Function<VertexQueryFactory, MatchPredicate> matchBuilder) {
        setter.accept(matchBuilder.apply(queryFactory));
        return this;
    }

}
