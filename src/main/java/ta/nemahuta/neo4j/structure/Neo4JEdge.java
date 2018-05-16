package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The gremlin adapter element for an {@link Edge}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JEdge extends Neo4JElement implements Edge {

    @Getter(onMethod = @__(@Nonnull))
    private final VertexOnEdgeSupplier inSupplier, outSupplier;

    public Neo4JEdge(@NonNull @Nonnull final Neo4JGraph graph,
                     @NonNull @Nonnull final Neo4JElementId<?> id,
                     @NonNull @Nonnull final ImmutableSet<String> labels,
                     @NonNull @Nonnull final Optional<MapAccessor> propertyAccessor,
                     @NonNull @Nonnull final AbstractPropertyFactory<? extends Neo4JProperty<? extends Neo4JElement, ?>> propertyFactory,
                     @NonNull @Nonnull final VertexOnEdgeSupplier inSupplier,
                     @NonNull @Nonnull final VertexOnEdgeSupplier outSupplier) {
        super(graph, id, labels, propertyAccessor, propertyFactory);
        this.inSupplier = inSupplier;
        this.outSupplier = outSupplier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        // transaction should be ready for io operations
        graph.tx().readWrite();
        return verticesStream(direction).iterator();
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return properties(Property::empty, propertyKeys);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return super.property(key, value, Property::empty);
    }

    private Stream<Vertex> verticesStream(final Direction direction) {
        switch (Optional.ofNullable(direction).orElse(Direction.BOTH)) {
            case OUT:
                return Stream.of(outSupplier).map(Supplier::get);
            case IN:
                return Stream.of(inSupplier).map(Supplier::get);
            default:
                return Stream.of(inSupplier, outSupplier).map(Supplier::get);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Graph graph() {
        return graph;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        // ElementHelper.areEqual is implemented on this.id(), handle the case of generated ids
        return object instanceof Edge && super.equals(object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
