package ta.nemahuta.neo4j.partition;

import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

/**
 * A partition in the graph, which can be used to separate parts of graphs.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JGraphPartition {

    /**
     * Ensure that the source orLabelsAnd include minimum of the partition orLabelsAnd.
     *
     * @param labels the source orLabelsAnd
     * @return the orLabelsAnd from the source including the ones from the partition
     */
    Set<String> ensurePartitionLabelsSet(@Nonnull Iterable<String> labels);

    /**
     * Ensure that the source orLabelsAnd do NOT include the minimum of partition orLabelsAnd.
     *
     * @param labels the source orLabelsAnd
     * @return the orLabelsAnd from the source excluding the ones from the partion
     */
    Set<String> ensurePartitionLabelsNotSet(@Nonnull Iterable<String> labels);

    /**
     * Create a new predicate to match the orLabelsAnd of the partitions for the provided alias in a where clause.
     *
     * @param alias the alias for the vertex
     * @return the {@link Optional} of the {@link WherePredicate} matching the labels
     */
    Optional<WherePredicate> vertexWhereLabelPredicate(@Nonnull String alias);

}
