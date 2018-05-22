package ta.nemahuta.neo4j.handler;

import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

public interface RelationProvider {

    /**
     * Provides the relations for a vertex id.
     *
     * @param lhsId     the left hand side vertex id
     * @param direction the direction for the query
     * @param labels    the labels to be used to filter (or an empty list if all are being queried)
     * @return a map of labels and their ids
     */
    Map<String, Set<Long>> loadRelatedIds(long lhsId, @Nonnull Direction direction, @Nonnull Set<String> labels);

}
