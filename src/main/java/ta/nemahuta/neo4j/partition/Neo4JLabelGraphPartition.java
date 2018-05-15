package ta.nemahuta.neo4j.partition;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.query.WherePredicate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract {@link Neo4JGraphPartition} using labels and an {@link OpMode} on them.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@RequiredArgsConstructor
public class Neo4JLabelGraphPartition implements Neo4JGraphPartition {

    public enum OpMode {
        OR, AND
    }

    @NonNull
    protected final Set<String> labelSet;

    @NonNull
    protected final OpMode opMode;


    @Override
    public Set<String> ensurePartitionLabelsSet(@Nonnull final Iterable<String> labels) {
        final ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder().addAll(labels);
        switch (opMode) {
            case OR:
                if (!labelSet.isEmpty()) {
                    builder.add(labelSet.iterator().next());
                }
                break;
            case AND:
                builder.addAll(labelSet);
                break;
        }
        return builder.build();
    }

    @Override
    public Set<String> ensurePartitionLabelsNotSet(@Nonnull final Iterable<String> labels) {
        return ImmutableSet.copyOf(StreamSupport.stream(labels.spliterator(), true).filter(l -> !labelSet.contains(l)).iterator());
    }

    @Override
    public Optional<WherePredicate> vertexWhereLabelPredicate(@Nonnull final String alias) {
        if (labelSet.isEmpty()) {
            return Optional.empty();
        }
        switch (opMode) {
            case AND:
                return Optional.of(whereAndPredicate(alias, labelSet));
            case OR:
            default:
                return labelSet.stream()
                        .map(Collections::singleton)
                        .map(andSets -> andSets.stream().map(andSet -> whereAndPredicate(alias, Collections.singleton(andSet))))
                        .reduce(Stream::concat)
                        .orElseGet(Stream::empty)
                        .reduce(WherePredicate::orOp);
        }
    }

    @Nonnull
    private WherePredicate whereAndPredicate(@Nonnull final String alias, final Iterable<String> labels) {
        return (queryBuilder, parameters) -> {
            queryBuilder.append(alias);
            labels.forEach(l -> queryBuilder.append(":").append("`").append(l).append("`"));
        };
    }

    /**
     * Create a partition which requires any label of the provided to be set.
     *
     * @param labels the labels which can be used
     * @return the partition
     */
    public static Neo4JGraphPartition anyLabelOf(final String... labels) {
        return new Neo4JLabelGraphPartition(ImmutableSet.copyOf(labels), OpMode.OR);
    }

    /**
     * Create a partition which requires at least one label to be set.
     *
     * @param labels the labels which have to be used
     * @return the partition
     */
    public static Neo4JGraphPartition allLabelsOf(final String... labels) {
        return new Neo4JLabelGraphPartition(ImmutableSet.copyOf(labels), OpMode.AND);
    }

    /**
     * @return a partition which is freely defined, thus no labels have to be used
     */
    public static Neo4JGraphPartition anyLabel() {
        return anyLabelOf();
    }

}
