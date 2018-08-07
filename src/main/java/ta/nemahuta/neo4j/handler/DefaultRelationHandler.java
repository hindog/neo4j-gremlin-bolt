package ta.nemahuta.neo4j.handler;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.tinkerpop.gremlin.structure.Direction;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.state.VertexEdgeReferences;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class DefaultRelationHandler implements RelationHandler {

    @NonNull
    private final Neo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder> vertexScope;
    @NonNull
    private final Neo4JElementStateScope<Neo4JEdgeState, EdgeQueryBuilder> edgeScope;

    @Override
    public Stream<Long> getRelationIdsOf(final long vertexId, @Nonnull final Direction direction, @Nonnull final Set<String> labels) {
        if (direction == Direction.BOTH) {
            // In case this queries both directions, concat the both direction streams
            return Stream.concat(getRelationIdsOf(vertexId, Direction.IN, labels), getRelationIdsOf(vertexId, Direction.OUT, labels));
        }
        return new VertexEdgeReferenceQuery(vertexScope, edgeScope, vertexId, direction, labels).relationIds();
    }

    @Override
    public void registerEdge(final long vertexId, @Nonnull final Direction direction, @Nonnull final String label, final long edgeId) {
        if (direction == Direction.BOTH) {
            registerEdge(vertexId, Direction.OUT, label, edgeId);
            registerEdge(vertexId, Direction.IN, label, edgeId);
        } else {
            Optional.ofNullable(vertexScope.get(vertexId))
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

}
