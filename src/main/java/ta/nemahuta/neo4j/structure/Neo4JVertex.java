package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import ta.nemahuta.neo4j.handler.RelationProvider;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Neo4J implementation of a {@link Vertex} for gremlin.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JVertex extends Neo4JElement<Neo4JVertexState, VertexProperty> implements Vertex {

    public static final String LABEL_DELIMITER = "::";

    private final RelationProvider edgeProvider;

    public Neo4JVertex(@Nonnull final Neo4JGraph graph, final long id,
                       @Nonnull final Neo4JElementStateScope<Neo4JVertexState, ? extends AbstractQueryBuilder> scope,
                       @Nonnull final RelationProvider edgeProvider) {
        super(graph, id, scope);
        this.edgeProvider = edgeProvider;
    }


    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (!(inVertex instanceof Neo4JVertex)) {
            throw new IllegalArgumentException("Cannot connect a " + getClass().getSimpleName() + " to a " +
                    Optional.ofNullable(inVertex).map(Object::getClass).map(Class::getSimpleName).orElse(null));
        }
        final Neo4JVertex neo4jInVertex = (Neo4JVertex) inVertex;
        return graph.addEdge(label, this, neo4jInVertex, keyValues);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Set<Long> edgeIds = edgeProvider.getRelationIdsOf(id, direction, ImmutableSet.copyOf(edgeLabels)).collect(Collectors.toSet());
        return edgeIds.isEmpty() ? Collections.emptyIterator() : graph.edges(edgeIds.toArray());
    }

    @Override
    public Iterator<Vertex> vertices(@Nonnull final Direction direction,
                                     @Nonnull final String... edgeLabels) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(edges(direction, edgeLabels), Spliterator.ORDERED), false)
                .map(e -> Objects.equals(e.inVertex().id(), this.id()) ? e.outVertex() : e.inVertex())
                .iterator();
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
