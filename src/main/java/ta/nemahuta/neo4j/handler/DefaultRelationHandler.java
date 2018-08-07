package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.edge.EdgeQueryFactory;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.state.VertexEdgeReferences;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class DefaultRelationHandler implements RelationHandler {

    @NonNull
    private final Neo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder> vertexScope;
    @NonNull
    private final HierarchicalCache<Long, Neo4JVertexState> vertexCache;
    @NonNull
    private final Neo4JElementStateScope<Neo4JEdgeState, EdgeQueryBuilder> edgeScope;

    @Override
    public Stream<Long> getRelationIdsOf(final long vertexId, @Nonnull final Direction direction, @Nonnull final Set<String> labels) {
        if (direction == Direction.BOTH) {
            // In case this queries both directions, concat the both direction streams
            return Stream.concat(getRelationIdsOf(vertexId, Direction.IN, labels), getRelationIdsOf(vertexId, Direction.OUT, labels));
        }
        return new VertexEdgeReferenceQuery(vertexId, direction, labels).relationIds();
    }

    @Override
    public void registerEdge(final long vertexId, @Nonnull final Direction direction, @Nonnull final String label, final long edgeId) {
        if (direction == Direction.BOTH) {
            registerEdge(vertexId, Direction.OUT, label, edgeId);
            registerEdge(vertexId, Direction.IN, label, edgeId);
        } else {
            Optional.ofNullable(vertexCache.get(vertexId))
                    // Apply the new edge
                    .ifPresent(state -> {
                        final VertexEdgeReferences references = state.getEdgeIds(direction);
                        final VertexEdgeReferences newReferences = references.withNewEdge(label, edgeId);
                        // And put the result to the cache, if changes occurred
                        if (newReferences != references) {
                            vertexScope.update(vertexId, state.withEdgeIds(direction, newReferences));
                        }
                    });
        }
    }

    protected class VertexEdgeReferenceQuery {

        private final long vertexId;
        private final Direction direction;

        private final Set<String> queriedLabels;

        private Neo4JVertexState state;

        public VertexEdgeReferenceQuery(@Nonnull final long vertexId,
                                        @Nonnull final Direction direction,
                                        @Nonnull final Set<String> queriedLabels) {
            if (direction == Direction.BOTH) {
                throw new IllegalArgumentException("Both edges are not supported.");
            }
            this.vertexId = vertexId;
            this.direction = direction;
            this.queriedLabels = queriedLabels;
            this.state = vertexScope.get(vertexId);
        }

        /**
         * @return the ids of the relations for this query
         */
        @Nonnull
        public Stream<Long> relationIds() {
            if (state == null) {
                // No state means no vertex, means no relations
                return Stream.empty();
            }
            // First we obtain all the labels
            final Set<String> labels = queriedLabels.isEmpty()
                    // if all labels should be used, use them from the reference or retrieve all edges
                    ? Optional.ofNullable((Set<String>) state.getEdgeIds(direction).getLabels()).orElseGet(() -> retrieveEdgeReferences(null).keySet())
                    // otherwise use the filter from provided
                    : queriedLabels;

            final Map<String, Set<Long>> results = new HashMap<>();
            // Fill up the result with non existing labels, if we know all of them
            Optional.ofNullable(state.getEdgeIds(direction).getLabels())
                    .ifPresent(existingLabels ->
                            Sets.difference(labels, existingLabels).forEach(notExisting -> results.put(notExisting, Collections.emptySet()))
                    );
            // First we try the references cache for the labels
            for (final String label : labels) {
                Optional.ofNullable(state.getEdgeIds(direction).get(label)).ifPresent(ids -> results.put(label, ids));
            }
            // For the remaining items, load them and put them to the map
            if (results.size() != labels.size()) {
                results.putAll(retrieveEdgeReferences(Sets.difference(labels, results.keySet())));
            }
            // Join all the results
            return results.values().stream().map(Set::stream).reduce(Stream::concat).orElseGet(Stream::empty);
        }

        @Nonnull
        private Map<String, Set<Long>> retrieveEdgeReferences(@Nullable final Set<String> labels) {
            final Map<String, Set<Long>> results =
                    edgeScope.queryAndCache(q -> q.labels(Optional.ofNullable(labels).map(ImmutableSet::copyOf).orElse(null))
                            .direction(direction)
                            .where(b -> b.getLhs().id(vertexId))
                            .andThen(EdgeQueryFactory::returnEdge))
                            .entrySet()
                            .stream()
                            .collect(Collectors.groupingBy(e -> e.getValue().getLabel(), Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));
            // Make sure we update the references
            final VertexEdgeReferences newReferences =
                    Optional.ofNullable(labels).map(partial -> state.getEdgeIds(direction).withPartialResolvedEdges(results))
                            .orElseGet(() -> state.getEdgeIds(direction).withAllResolvedEdges(results));
            update(newReferences);

            return results;
        }

        private void update(@Nonnull final VertexEdgeReferences references) {
            Optional.ofNullable(state).ifPresent(s -> vertexScope.update(vertexId, this.state = s.withEdgeIds(direction, references)));
        }

    }
}
