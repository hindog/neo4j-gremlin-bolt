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
import java.util.HashSet;
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

        final Map<String, Set<Long>> newMap = new HashMap<>(this.edgeIdsPerLabel);
        final ImmutableSet<Long> ids = this.edgeIdsPerLabel.get(label);

        if (ids != null && !ids.contains(edgeId)) {
            // we have references for that label and they do not contain the edge id to be added
            final Set<Long> combined = new HashSet<>(ids);
            combined.add(edgeId);
            newMap.put(label, combined);
        } else if (ids == null && this.labels != null) {
            // we do not have references, but all labels are loaded
            newMap.put(label, ImmutableSet.of(edgeId));
        } else {
            // otherwise we have nothing to be done here
            return this;
        }

        return withPartialResolvedEdges(newMap);
    }

    /**
     * Mark all edge references to be loaded and create a new instance which uses the resolved edges.
     *
     * @param allEdgeReferences all resolved edge references
     * @return the new instance containing all the resolved references
     */
    @Nonnull
    public VertexEdgeReferences withAllResolvedEdges(@Nonnull final Map<String, Set<Long>> allEdgeReferences) {
        final ImmutableMap<String, ImmutableSet<Long>> result = convertMap(allEdgeReferences);
        return new VertexEdgeReferences(result.keySet(), result);
    }

    /**
     * Mark some edges to be resolved.
     *
     * @param someEdgeReferences some resolved edge references to be merged into the current ones
     * @return the new instance containing the new knowledge
     */
    @Nonnull
    public VertexEdgeReferences withPartialResolvedEdges(@Nonnull final Map<String, Set<Long>> someEdgeReferences) {
        final Map<String, Set<Long>> merge = new HashMap<>(this.edgeIdsPerLabel);
        merge.putAll(someEdgeReferences);
        final ImmutableMap<String, ImmutableSet<Long>> edgeIdsPerLabel = convertMap(merge);
        final ImmutableSet<String> labels = this.labels != null ? edgeIdsPerLabel.keySet() : null;
        return new VertexEdgeReferences(labels, edgeIdsPerLabel);
    }

    /**
     * Removes all edges with the provided ids.
     *
     * @param removedEdgeIds the ids of the provided edges
     * @return this, if no change occurred, or a new instance not containing the removed ids
     */
    public VertexEdgeReferences withRemovedEdges(final Set<Long> removedEdgeIds) {
        if (getAllKnown().noneMatch(removedEdgeIds::contains)) {
            return this;
        }
        return withPartialResolvedEdges(Maps.transformValues(this.edgeIdsPerLabel, v -> Sets.difference(v, removedEdgeIds)));
    }

    private ImmutableMap<String, ImmutableSet<Long>> convertMap(@Nonnull final Map<String, Set<Long>> source) {
        final ImmutableMap.Builder<String, ImmutableSet<Long>> builder = ImmutableMap.builder();
        source.forEach((k, v) -> builder.put(k, ImmutableSet.copyOf(v)));
        return builder.build();
    }

}
