package ta.nemahuta.neo4j.partition;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * {@link Neo4JGraphPartition} using a minimum set of labels.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class Neo4JLabelGraphPartition implements Neo4JGraphPartition {

    @NonNull
    protected final Set<String> minimumLabels;

    @Override
    public Set<String> ensurePartitionLabelsSet(@Nonnull final Iterable<String> labels) {
        return ImmutableSet.<String>builder().addAll(labels).addAll(minimumLabels).build();
    }

    @Override
    public Set<String> ensurePartitionLabelsNotSet(@Nonnull final Iterable<String> labels) {
        return ImmutableSet.copyOf(StreamSupport.stream(labels.spliterator(), true).filter(l -> !minimumLabels.contains(l)).iterator());
    }

    @Override
    public Optional<WherePredicate> vertexWhereLabelPredicate(@Nonnull final String alias) {
        if (minimumLabels.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of((queryBuilder, parameters) -> {
                queryBuilder.append(alias);
                minimumLabels.forEach(l -> queryBuilder.append(":").append("`").append(l).append("`"));
            });
        }
    }

    /**
     * Create a partition which requires at least one label to be set.
     *
     * @param labels the labels which have to be used
     * @return the partition
     */
    public static Neo4JGraphPartition allLabelsOf(final String... labels) {
        return new Neo4JLabelGraphPartition(ImmutableSet.copyOf(labels));
    }

    /**
     * @return a partition which is freely defined, thus no labels have to be used
     */
    public static Neo4JGraphPartition anyLabel() {
        return allLabelsOf();
    }

}
