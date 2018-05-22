package ta.nemahuta.neo4j.cache;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

@RequiredArgsConstructor
public abstract class DefaultSessionCache implements SessionCache {

    @Getter
    private final HierarchicalCache<Long, Neo4JEdgeState> edgeCache;
    @Getter
    private final HierarchicalCache<Long, Neo4JVertexState> vertexCache;

}
