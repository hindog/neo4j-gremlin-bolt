package ta.nemahuta.neo4j.session;

import ta.nemahuta.neo4j.session.scope.Neo4JEdgeScope;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;

/**
 * Scope for all session elements.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface SessionScope extends RollbackAndCommit {

    /**
     * @return the {@link Neo4JElementScope} for {@link Neo4JVertex}es
     */
    @Nonnull
    Neo4JElementScope<Neo4JVertex> getVertexScope();

    /**
     * @return the {@link Neo4JElementScope} for {@link Neo4JEdge}es
     */
    @Nonnull
    Neo4JEdgeScope getEdgeScope();

}
