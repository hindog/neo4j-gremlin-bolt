package ta.nemahuta.neo4j.cache;

import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

/**
 * Interface for a session cache.
 */
public interface SessionCache extends AutoCloseable {

    /**
     * @return the {@link HierarchicalCache} for {@link Neo4JEdgeState}s
     */
    HierarchicalCache<Long, Neo4JEdgeState> getEdgeCache();

    /**
     * @return the {@link HierarchicalCache} for {@link Neo4JVertexState}s
     */
    HierarchicalCache<Long, Neo4JVertexState> getVertexCache();

    void close();

}
