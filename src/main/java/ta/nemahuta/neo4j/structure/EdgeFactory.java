package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;

/**
 * Functional interface for a factory for {@link Neo4JEdge}s.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@FunctionalInterface
public interface EdgeFactory {

    /**
     * Construct a new {@link Neo4JEdge} for the provided parameters.
     * @param label the label of the edge
     * @param outVertex the outbound vertex
     * @param inVertex the inbound vertex
     * @param keyValues the key value pairs to be set as properties
     * @return the new edge
     */
    @Nonnull
    Neo4JEdge createEdge(@Nonnull final String label, @Nonnull final Neo4JVertex outVertex,
                         @Nonnull final Vertex inVertex, @Nonnull final Object... keyValues);

}
