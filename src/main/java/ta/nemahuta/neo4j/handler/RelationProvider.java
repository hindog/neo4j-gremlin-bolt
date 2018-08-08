package ta.nemahuta.neo4j.handler;

import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;

public interface RelationProvider {
    /**
     * Provides the relation ids for a vertex id.
     *
     * @param vertexId  the vertex id
     * @param direction the direction for the query
     * @param labels    the labels to be used to filter (or an empty list if all are being queried)
     * @return a map of labels and the corresponding ids of the edges
     */
    @Nonnull
    Stream<Long> getRelationIdsOf(long vertexId, @Nonnull Direction direction, @Nonnull Set<String> labels);

}
