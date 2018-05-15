package ta.nemahuta.neo4j.query.vertex;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.*;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@link StatementBuilder} and {@link VertexQueryFactory} for {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class VertexQueryBuilder extends AbstractQueryBuilder {

    /**
     * The alias of the vertex when querying
     */
    public static final String ALIAS = "v";
    /**
     * The parameter name of the vertex properties in the query
     */
    public static final String PARAM_PROPERTIES = "vp";

    private final VertexQueryFactory factory = new VertexQueryFactory() {

        @Getter(AccessLevel.PROTECTED)
        private final UniqueParamNameGenerator paramNameGenerator = new UniqueParamNameGenerator();

        @Override
        protected Neo4JElementIdAdapter<?> getIdAdapter() {
            return VertexQueryBuilder.this.idAdapter;
        }

        @Override
        protected String getAlias() {
            return ALIAS;
        }

    };

    /**
     * Create a new builder for the provided parameters.
     *
     * @param idProvider the identifier provider to be used
     * @param partition  the readPartition to be used
     */
    public VertexQueryBuilder(@Nonnull @NonNull final Neo4JElementIdAdapter<?> idProvider,
                              @Nonnull @NonNull final Neo4JGraphPartition partition) {
        super(idProvider, partition);
    }

    /**
     * Set the {@link MatchPredicate}.
     *
     * @param matchBuilder a function which builds the predicate from the {@link VertexQueryFactory}
     * @return {@code this}
     */
    @Nonnull
    public VertexQueryBuilder match(@Nonnull @NonNull final Function<VertexQueryFactory, MatchPredicate> matchBuilder) {
        setMatch(matchBuilder.apply(factory));
        return this;
    }

    /**
     * Set the {@link WherePredicate}.
     *
     * @param whereBuilder a function which builds the predicate from the {@link VertexQueryFactory}
     * @return {@code this}
     */
    @Nonnull
    public VertexQueryBuilder where(@Nonnull @NonNull final Function<VertexQueryFactory, WherePredicate> whereBuilder) {
        setWhere(whereBuilder.apply(factory));
        return this;
    }

    /**
     * Add an operation to be executed.
     *
     * @param opBuilder a function which builds the operation from the {@link VertexQueryFactory}
     * @return {@code this}
     */
    @Nonnull
    public VertexQueryBuilder andThen(@Nonnull @NonNull final Function<VertexQueryFactory, VertexOperation> opBuilder) {
        addOperation(opBuilder.apply(factory));
        return this;
    }

    @Override
    protected WherePredicate getWhere() {
        return Optional.ofNullable(super.getWhere())
                .map(w -> partition.vertexWhereLabelPredicate(ALIAS).map(p -> w.and(p)).orElse(w))
                .orElse(null);
    }
}
