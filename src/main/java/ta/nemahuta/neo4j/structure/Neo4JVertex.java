/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.types.Node;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.Neo4JSession;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.PropertyCardinality;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
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

    private final AbstractNeo4JPropertyAccessors<Neo4JVertex, Neo4JVertexProperty<?>> properties =
            new AbstractNeo4JPropertyAccessors<Neo4JVertex, Neo4JVertexProperty<?>>() {
                @Override
                protected Neo4JVertexProperty<?> createProperty(@Nonnull final String name) {
                    return new Neo4JVertexProperty<>(Neo4JVertex.this, name);
                }
            };

    private final Neo4JElementScope<Neo4JVertex> scope;
    private final EdgeProvider inEdgeProvider, outEdgeProvider;
    private final EdgeFactory edgeFactory;

    /**
     * Constructs a new vertex for a {@link Neo4JGraph}, {@link Neo4JSession} and a {@link StateHolder}.
     *
     * @param graph       the graph the vertex is part of
     * @param scope       the scope
     * @param stateHolder the state holder for the vertex
     */
    protected Neo4JVertex(@Nonnull @NonNull final Neo4JGraph graph,
                          @Nonnull @NonNull final Neo4JElementScope<Neo4JVertex> scope,
                          @Nonnull @NonNull final StateHolder<Neo4JElementState> stateHolder,
                          @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> inEdgeProviderFactory,
                          @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> outEdgeProviderFactory,
                          @Nonnull @NonNull final EdgeFactory edgeFactory) {
        super(graph, stateHolder);
        this.scope = scope;
        this.inEdgeProvider = inEdgeProviderFactory.apply(this);
        this.outEdgeProvider = outEdgeProviderFactory.apply(this);
        this.edgeFactory = edgeFactory;

    }

    /**
     * Create a new transient vertex.
     *
     * @param graph  the graph the vertex is part of
     * @param scope  the scope
     * @param labels the initial orLabelsAnd for the vertex
     */
    public Neo4JVertex(@Nonnull @NonNull final Neo4JGraph graph,
                       @Nonnull @NonNull final Neo4JElementScope<Neo4JVertex> scope,
                       @Nonnull @NonNull final Collection<String> labels,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> inEdgeProviderFactory,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> outEdgeProviderFactory,
                       @Nonnull @NonNull final EdgeFactory edgeFactory) {

        this(graph, scope,
                new StateHolder<>(SyncState.TRANSIENT,
                        new Neo4JElementState(scope.getIdAdapter().generate(),
                                ImmutableSet.copyOf(labels),
                                ImmutableMap.of())
                ),
                inEdgeProviderFactory, outEdgeProviderFactory, edgeFactory
        );
    }

    /**
     * Create a vertex from an existing node.
     *
     * @param graph the graph the vertex is part of
     * @param scope the scope
     * @param node  the node which provides the orLabelsAnd and properties for this vertex
     */
    public Neo4JVertex(@Nonnull @NonNull final Neo4JGraph graph,
                       @Nonnull @NonNull final Neo4JElementScope<Neo4JVertex> scope,
                       @Nonnull @NonNull final Node node,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> inEdgeProviderFactory,
                       @Nonnull @NonNull final Function<Neo4JVertex, EdgeProvider> outEdgeProviderFactory,
                       @Nonnull @NonNull final EdgeFactory edgeFactory) {

        this(graph, scope,
                new StateHolder<>(SyncState.SYNCHRONOUS,
                        new Neo4JElementState(scope.getIdAdapter().retrieveId(node),
                                ImmutableSet.copyOf(node.labels()),
                                PropertyValueFactory.forScope(scope).create(node)
                        )
                ), inEdgeProviderFactory, outEdgeProviderFactory, edgeFactory
        );
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
        return Stream.of(propertyKeys).map(k -> (VertexProperty<V>) properties.get(k)).iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final java.lang.Object... keyValues) {
        final Neo4JVertexProperty<V> result = (Neo4JVertexProperty<V>) properties.get(key);
        result.setValue(scope.getPropertyIdGenerator(), PropertyCardinality.from(cardinality), value);
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
