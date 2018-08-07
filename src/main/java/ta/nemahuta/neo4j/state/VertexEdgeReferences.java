package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class VertexEdgeReferences implements Serializable {

    @Getter
    @Nullable
    private final ImmutableSet<String> labels;

    @NonNull
    @Nonnull
    private final ImmutableMap<String, ImmutableSet<Long>> edgeIdsPerLabel;

    public VertexEdgeReferences() {
        this(null, ImmutableMap.of());
    }

    @Nonnull
    public Stream<Long> getAllKnown() {
        return edgeIdsPerLabel.values().stream().map(Set::stream).reduce(Stream::concat).orElseGet(Stream::empty);
    }

    @Nonnull
    public ImmutableMap<String, ImmutableSet<Long>> edgeIds(@Nonnull final Set<String> labels) {
        // We need the labels to be present
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("Labels to be queried must not be empty.");
        }
        // Now with the labels present, create a new map
        final ImmutableMap.Builder<String, ImmutableSet<Long>> builder = ImmutableMap.builder();
        for (final String label : labels) {
            //
            Optional.ofNullable(edgeIdsPerLabel.get(label)) // Querying the values
                    .map(Optional::of)
                    // If no ids are known, but we have complete knowledge of the edges, we use an empty set
                    .orElseGet(() -> Optional.ofNullable(this.labels).map(l -> ImmutableSet.of()))
                    // Put the resulting ids into the builder
                    .ifPresent(v -> builder.put(label, v));
        }
        return builder.build();
    }

    @Nullable
    public ImmutableSet<Long> get(@Nonnull final String label) {
        return edgeIdsPerLabel.get(label);
    }

    @Nonnull
    public VertexEdgeReferences withNewEdge(@Nonnull final String label, final long edgeId) {
        final ImmutableSet<Long> ids = edgeIdsPerLabel.get(label);

        // Make sure the label gets known, in case all labels are known and the label is not yet known
        final ImmutableSet<String> newLabels = (labels != null && !this.labels.contains(label))
                ? ImmutableSet.<String>builder().addAll(this.labels).add(label).build()
                : this.labels;

        final Map<String, ImmutableSet<Long>> newMap = new HashMap<>(this.edgeIdsPerLabel);

        if (ids != null && !ids.contains(edgeId)) {
            // we have references for that label and they do not contain the edge id to be added
            newMap.put(label, ImmutableSet.<Long>builder().addAll(ids).add(edgeId).build());
        } else if (ids == null && this.labels != null) {
            // we do not have references, but all labels are loaded
            newMap.put(label, ImmutableSet.of(edgeId));
        } else {
            // otherwise we have nothing to be done here
            return this;
        }
        return new VertexEdgeReferences(newLabels, ImmutableMap.copyOf(newMap));
    }

    /**
     * Mark all edge references to be loaded and create a new instance which uses the resolved edges.
     *
     * @param allEdgeReferences all resolved edge references
     * @return the new instance containing all the resolved references
     */
    @Nonnull
    public VertexEdgeReferences withAllResolvedEdges(@Nonnull final Map<String, Set<Long>> allEdgeReferences) {
        final ImmutableMap.Builder<String, ImmutableSet<Long>> builder = ImmutableMap.builder();
        allEdgeReferences.forEach((k, v) -> builder.put(k, ImmutableSet.copyOf(v)));
        return new VertexEdgeReferences(ImmutableSet.copyOf(allEdgeReferences.keySet()), builder.build());
    }

    /**
     * Mark some edges to be resolved.
     *
     * @param someEdgeReferences some resolved edge references to be merged into the current ones
     * @return the new instance containing the new knowledge
     */
    @Nonnull
    public VertexEdgeReferences withPartialResolvedEdges(@Nonnull final Map<String, Set<Long>> someEdgeReferences) {
        final ImmutableMap.Builder<String, ImmutableSet<Long>> builder = ImmutableMap.builder();
        this.edgeIdsPerLabel.entrySet().stream().filter(e -> !someEdgeReferences.containsKey(e.getKey())).forEach(builder::put);
        return new VertexEdgeReferences(
                Optional.ofNullable(this.labels)// In case we know all labels
                        .map(ImmutableSet.<String>builder()::addAll)// Add the current knowledge
                        .map(b -> b.addAll(Sets.difference(this.labels, someEdgeReferences.keySet()))) // Add all new labels as well
                        .map(ImmutableSet.Builder::build) // And build the set
                        .orElse(null), // Fall back to no labels known
                ImmutableMap.<String, ImmutableSet<Long>>builder().build()
        );
    }

    public VertexEdgeReferences withRemovedEdges(final Set<Long> removedIds) {
        if (edgeIdsPerLabel.values().stream().noneMatch(removedIds::contains)) {
            return this;
        }
        return new VertexEdgeReferences(this.labels,
                ImmutableMap.copyOf(Maps.transformValues(this.edgeIdsPerLabel,
                        v -> v.stream().filter(id -> !removedIds.contains(id)).collect(ImmutableSet.toImmutableSet())))
        );
    }
}
