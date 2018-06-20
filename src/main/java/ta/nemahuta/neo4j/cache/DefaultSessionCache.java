package ta.nemahuta.neo4j.cache;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.scope.IdCache;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;

@RequiredArgsConstructor
public class DefaultSessionCache implements SessionCache {

    @Getter(onMethod = @__(@Nonnull))
    @NonNull
    private final HierarchicalCache<Long, Neo4JEdgeState> edgeCache;

    @Getter(onMethod = @__(@Nonnull))
    @NonNull
    private final IdCache<Long> knownEdgeIds;

    @Getter(onMethod = @__(@Nonnull))
    @NonNull
    private final HierarchicalCache<Long, Neo4JVertexState> vertexCache;

    @Getter(onMethod = @__(@Nonnull))
    @NonNull
    private final IdCache<Long> knownVertexIds;

}
