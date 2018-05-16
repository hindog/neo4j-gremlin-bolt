package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Neo4J implementation of a {@link Vertex} for gremlin.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JVertex extends Neo4JElement implements Vertex {

    public static final String LabelDelimiter = "::";

    private final EdgeProvider inEdgeProvider, outEdgeProvider;
    private final EdgeFactory edgeFactory;

    public Neo4JVertex(@NonNull @Nonnull final Neo4JGraph graph,
                       @NonNull @Nonnull final Neo4JElementId<?> id,
                       @NonNull @Nonnull final ImmutableSet<String> labels,
                       @NonNull @Nonnull final Optional<MapAccessor> propertyAccessor,
                       @NonNull @Nonnull final AbstractPropertyFactory<? extends Neo4JProperty<? extends Neo4JElement, ?>> propertyFactory,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> inEdgeProviderFactory,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> outEdgeProviderFactory,
                       @Nonnull @NonNull final EdgeFactory edgeFactory) {
        super(graph, id, labels, propertyAccessor, propertyFactory);
        this.inEdgeProvider = inEdgeProviderFactory.apply(this);
        this.outEdgeProvider = outEdgeProviderFactory.apply(this);
        this.edgeFactory = edgeFactory;
    }


    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        final Neo4JEdge result = edgeFactory.createEdge(label, this, inVertex, keyValues);
        outEdgeProvider.registerEdge(result);
        return result;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return distinctEdges(direction, edgeLabels).iterator();
    }

    @Nonnull
    private Stream<Edge> distinctEdges(@Nonnull @NonNull final Direction direction,
                                       @Nonnull @NonNull final String... edgeLabels) {
        return edgeProviderStream(direction)
                .map(p -> (Edge) p.provideEdges(edgeLabels))
                .distinct();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return distinctEdges(direction, edgeLabels)
                .map(e -> vertexOf(direction, e))
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .filter(v -> !Objects.equals(this, v))
                .iterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return properties(VertexProperty::empty, propertyKeys);
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality,
                                          final String key,
                                          final V value,
                                          final java.lang.Object... keyValues) {
        final VertexProperty<V> result = property(key, value, VertexProperty::empty);
        ElementHelper.attachProperties(result, keyValues);
        return result;
    }

    @Nonnull
    private Stream<EdgeProvider> edgeProviderStream(final Direction direction) {
        switch (Optional.ofNullable(direction).orElse(Direction.BOTH)) {
            case IN:
                return Stream.of(inEdgeProvider);
            case OUT:
                return Stream.of(outEdgeProvider);
            case BOTH:
                return Stream.of(inEdgeProvider, outEdgeProvider);
            default:
                return throwDirectionNotHandled(direction);
        }
    }

    @Nonnull
    private Stream<Vertex> vertexOf(@Nonnull @NonNull final Direction direction,
                                    @Nonnull @NonNull final Edge edge) {
        switch (direction) {
            case IN:
                return Stream.of(edge.outVertex());
            case OUT:
                return Stream.of(edge.inVertex());
            case BOTH:
                return Stream.of(edge.inVertex(), edge.outVertex());
            default:
                return throwDirectionNotHandled(direction);
        }
    }

    private <T> T throwDirectionNotHandled(@Nullable @NonNull final Direction direction) {
        throw new IllegalStateException("Cannot handle direction: " + direction);
    }


}
