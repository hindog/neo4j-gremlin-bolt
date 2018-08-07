package ta.nemahuta.neo4j.handler;

import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;

public interface RelationHandler extends RelationProvider {

    /**
     * Registers an edge for a vertex using a direction and label.
     *
     * @param vertexId  the id of the vertex to be registered
     * @param direction the direction of the edge
     * @param label     the label of the edge
     * @param edgeId    the id of the edge
     */
    void registerEdge(long vertexId, @Nonnull Direction direction, @Nonnull String label, long edgeId);

}
