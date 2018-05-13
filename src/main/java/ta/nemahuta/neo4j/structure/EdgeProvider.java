package ta.nemahuta.neo4j.structure;

import javax.annotation.Nonnull;

/**
 * Interface for an edge provider.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface EdgeProvider {

    /**
     * Provides the edges matching the orLabelsAnd.
     *
     * @param labels the orLabelsAnd, or an empty array for each and every edge
     * @return the edges for the label
     */
    @Nonnull
    Iterable<Neo4JEdge> provideEdges(@Nonnull String... labels);

    /**
     * Registers an edge.
     *
     * @param edge the edge to be registered
     */
    void registerEdge(@Nonnull Neo4JEdge edge);

}
