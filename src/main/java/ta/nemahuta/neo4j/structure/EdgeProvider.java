package ta.nemahuta.neo4j.structure;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Interface for an edge provider.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface EdgeProvider {

    /**
     * Provides the edge ids matching the labels
     *
     * @param labels the orLabelsAnd, or an empty array for each and every edge
     * @return the edges for the label
     */
    @Nonnull
    Collection<Long> provideEdges(@Nonnull String... labels);

    /**
     * Registers an edge.
     *
     * @param label the label of the edge
     * @param id    the identifier of the edges
     */
    void register(@Nonnull String label, long id);

}
