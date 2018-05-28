package ta.nemahuta.neo4j.query.vertex;

import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.query.MatchPredicate;
import ta.nemahuta.neo4j.query.UniqueParamNameGenerator;
import ta.nemahuta.neo4j.query.WherePredicate;
import ta.nemahuta.neo4j.query.predicate.WhereIdInPredicate;
import ta.nemahuta.neo4j.query.vertex.predicate.MatchAllVertexLabelsPredicate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Abstract factory for {@link ta.nemahuta.neo4j.query.QueryPredicate}s of {@link ta.nemahuta.neo4j.structure.Neo4JVertex}es.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public abstract class VertexQueryPredicateFactory {

    /**
     * @return the alias of the vertex
     */
    @Nonnull
    protected abstract String getAlias();

    /**
     * @return the graph partition the query should work on
     */
    @Nonnull
    protected abstract Neo4JGraphPartition getPartition();

    /**
     * Construct a predicate matching the vertex id in a where clause.
     *
     * @param ids the id of the vertex
     * @return the predicate
     */
    @Nonnull
    public WherePredicate idsInSet(@Nonnull final Set<Long> ids) {
        return ids.isEmpty() ? WherePredicate.EMPTY : new WhereIdInPredicate(ids, getAlias(), getParamNameGenerator().generate("vertexId"));
    }

    /**
     * Construct a predicate matching the vertex id in a where clause.
     *
     * @param id the id of the vertex
     * @return the predicate
     */
    @Nonnull
    public WherePredicate id(final long id) {
        return idsInSet(Collections.singleton(id));
    }

    /**
     * Construct a predicate matching the orLabelsAnd provided.
     *
     * @param labels the orLabelsAnd to be matched
     * @return the predicate
     */
    @Nonnull
    public MatchPredicate labelsMatch(@Nonnull final Set<String> labels) {
        return new MatchAllVertexLabelsPredicate(labels, getPartition(), getAlias());
    }

    /**
     * @return the parameter name generator
     */
    @Nonnull
    protected abstract UniqueParamNameGenerator getParamNameGenerator();

}
