package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.types.Relationship;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.PropertyCardinality;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;

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

    private final Neo4JElementScope<Neo4JEdge> scope;
    @Getter(onMethod = @__(@Nonnull))
    private final VertexOnEdgeSupplier inSupplier, outSupplier;

    private final AbstractNeo4JPropertyAccessors<Neo4JEdge, Neo4JEdgeProperty<?>> properties =
            new AbstractNeo4JPropertyAccessors<Neo4JEdge, Neo4JEdgeProperty<?>>() {
                @Override
                protected Neo4JEdgeProperty<?> createProperty(@Nonnull final String name) {
                    return new Neo4JEdgeProperty<>(Neo4JEdge.this, name);
                }
            };

    /**
     * Create a new edge.
     *
     * @param graph       the graph the edge is part of
     * @param scope       the session the edge is registered in
     * @param stateHolder the state holder for the edge
     * @param inSupplier  the incoming vertex supplier
     * @param outSupplier the outgoing vertex supplier
     */
    protected Neo4JEdge(@Nonnull @NonNull final Neo4JGraph graph,
                        @Nonnull @NonNull final StateHolder<Neo4JElementState> stateHolder,
                        @Nonnull @NonNull final Neo4JElementScope<Neo4JEdge> scope,
                        @Nonnull @NonNull final VertexOnEdgeSupplier inSupplier,
                        @Nonnull @NonNull final VertexOnEdgeSupplier outSupplier) {
        super(graph, stateHolder);
        this.scope = scope;
        this.inSupplier = inSupplier;
        this.outSupplier = outSupplier;
    }

    /**
     * Create a new transient edge.
     *
     * @param graph       the graph the edge is part of
     * @param scope       the scope for the edge
     * @param label       the label for the edge
     * @param inSupplier  the incoming vertex supplier
     * @param outSupplier the outgoing vertex supplier
     */
    public Neo4JEdge(@Nonnull @NonNull final Neo4JGraph graph,
                     @Nonnull @NonNull final String label,
                     @Nonnull @NonNull final Neo4JElementScope<Neo4JEdge> scope,
                     @Nonnull @NonNull final VertexOnEdgeSupplier inSupplier,
                     @Nonnull @NonNull final VertexOnEdgeSupplier outSupplier) {
        this(graph,
                new StateHolder<>(SyncState.TRANSIENT,
                        new Neo4JElementState(scope.getIdAdapter().generate(), ImmutableSet.of(label), ImmutableMap.of())
                ), scope, inSupplier, outSupplier
        );
    }

    /**
     * Create a new edge from an existing one in the graphdb.
     *
     * @param graph        the graph the edge is part of
     * @param scope        the scope for the edge
     * @param relationship the source for the label and properties
     * @param inSupplier   the incoming vertex supplier
     * @param outSupplier  the outgoing vertex supplier
     */
    public Neo4JEdge(@Nonnull @NonNull final Neo4JGraph graph,
                     @Nonnull @NonNull final Relationship relationship,
                     @Nonnull @NonNull final Neo4JElementScope<Neo4JEdge> scope,
                     @Nonnull @NonNull final VertexOnEdgeSupplier inSupplier,
                     @Nonnull @NonNull final VertexOnEdgeSupplier outSupplier) {
        this(graph,
                new StateHolder<>(SyncState.SYNCHRONOUS,
                        new Neo4JElementState(scope.getIdAdapter().retrieveId(relationship), ImmutableSet.of(relationship.type()),
                                PropertyValueFactory.forScope(scope).create(relationship))
                ), scope, inSupplier, outSupplier
        );
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
        return Stream.of(propertyKeys).map(k -> (Property<V>) this.properties.get(k)).iterator();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        final Neo4JEdgeProperty<V> property = (Neo4JEdgeProperty<V>) properties.get(key);
        property.setValue(scope.getPropertyIdGenerator(), PropertyCardinality.SINGLE, value);
        return property;
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
