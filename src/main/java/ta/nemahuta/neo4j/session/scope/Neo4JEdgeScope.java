package ta.nemahuta.neo4j.session.scope;

import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * Additional interface extending {@link Neo4JElementScope} for {@link Neo4JEdge}s adding functionality to retrieve edges.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JEdgeScope extends Neo4JElementScope<Neo4JEdge> {

    /**
     * Retrieve the inbound edges for the provided parameters.
     *
     * @param graph  the graph for the edges
     * @param v      the vertex the inbound edges should be retrieved for
     * @param labels the labels of the edges to be retrieved or an empty {@link Iterable} for every label
     * @return a {@link Stream} of {@link Neo4JEdge}s matching the criteria
     */
    Stream<Neo4JEdge> inEdgesOf(@Nonnull Neo4JGraph graph, @Nonnull Neo4JVertex v, @Nonnull Iterable<String> labels);

    /**
     * Retrieve the outbound edges for the provided parameters.
     *
     * @param graph  the graph for the edges
     * @param v      the vertex the outbound edges should be retrieved for
     * @param labels the labels of the edges to be retrieved or an empty {@link Iterable} for every label
     * @return a {@link Stream} of {@link Neo4JEdge}s matching the criteria
     */
    Stream<Neo4JEdge> outEdgesOf(@Nonnull Neo4JGraph graph, @Nonnull Neo4JVertex v, @Nonnull Iterable<String> labels);

}
