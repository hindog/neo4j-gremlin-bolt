package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Neo4J implementation of a {@link Vertex} for gremlin.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JVertex extends Neo4JElement<Neo4JVertexState, VertexProperty> implements Vertex {

    public static final String LABEL_DELIMITER = "::";

    private final EdgeProvider inEdgeProvider, outEdgeProvider;

    public Neo4JVertex(@Nonnull final Neo4JGraph graph, final long id,
                       @Nonnull final Neo4JElementStateScope<Neo4JVertexState> scope,
                       @Nonnull final EdgeProvider inEdgeProvider,
                       @Nonnull final EdgeProvider outEdgeProvider) {
        super(graph, id, scope);
        this.inEdgeProvider = inEdgeProvider;
        this.outEdgeProvider = outEdgeProvider;
    }


    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (!(inVertex instanceof Neo4JVertex)) {
            throw new IllegalArgumentException("Cannot connect a " + getClass().getSimpleName() + " to a " +
                    Optional.ofNullable(inVertex).map(Object::getClass).map(Class::getSimpleName).orElse(null));
        }
        final Neo4JEdge result = graph.addEdge(label, this, (Neo4JVertex) inVertex, keyValues);
        outEdgeProvider.register(label, result.id());
        return result;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Set<Long> edgeIds = edgeIdStream(direction, edgeLabels).collect(Collectors.toSet());
        return edgeIds.isEmpty() ? Collections.emptyIterator() : graph.edges(edgeIds.toArray());
    }

    @Override
    public Iterator<Vertex> vertices(@Nonnull final Direction direction,
                                     @Nonnull final String... edgeLabels) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(edges(direction, edgeLabels), Spliterator.ORDERED), false)
                .map(e -> Objects.equals(e.inVertex().id(), this.id()) ? e.outVertex() : e.inVertex())
                .iterator();
    }

    private Stream<Long> edgeIdStream(final Direction direction, final String... edgeLabels) {
        switch (Optional.ofNullable(direction).orElse(Direction.BOTH)) {
            case IN:
                return inEdgeIdStream(edgeLabels);
            case OUT:
                return outEdgeIdStream(edgeLabels);
            default:
            case BOTH:
                return Stream.concat(inEdgeIdStream(edgeLabels), outEdgeIdStream(edgeLabels));
        }
    }

    private Stream<Long> inEdgeIdStream(@Nonnull final String... labels) {
        return inEdgeProvider.provideEdges(labels).stream();
    }

    private Stream<Long> outEdgeIdStream(@Nonnull final String... labels) {
        return outEdgeProvider.provideEdges(labels).stream();
    }

    @Override
    public String label() {
        return getState().getLabels().stream().collect(Collectors.joining(LABEL_DELIMITER));
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return getProperties(propertyKeys).map(p -> (VertexProperty<V>) p).iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality,
                                          final String key,
                                          final V value,
                                          final java.lang.Object... keyValues) {
        final VertexProperty result = getProperty(key, value);
        ElementHelper.attachProperties(result, keyValues);
        return result;
    }

    @Override
    protected VertexProperty createNewProperty(final String key) {
        return new Neo4JVertexProperty(this, key);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

}
