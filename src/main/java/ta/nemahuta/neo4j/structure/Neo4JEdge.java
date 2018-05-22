package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;

/**
 * The gremlin adapter element for an {@link Edge}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JEdge extends Neo4JElement<Neo4JEdgeState, Property> implements Edge {

    public Neo4JEdge(@Nonnull final Neo4JGraph graph,
                     @Nonnull final long id,
                     @Nonnull final Neo4JElementStateScope<Neo4JEdgeState> scope) {
        super(graph, id, scope);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return graph.vertices(idsForDirection(direction));
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return getProperties(propertyKeys).map(p -> (Property<V>) p).iterator();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return getProperty(key, value);
    }

    @Override
    public String label() {
        return getState().getLabel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        return object instanceof Edge && super.equals(object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    private Object[] idsForDirection(final Direction direction) {
        switch (Optional.ofNullable(direction).orElse(Direction.BOTH)) {
            case OUT:
                return new Long[]{getState().getOutVertexId()};
            case IN:
                return new Long[]{getState().getInVertexId()};
            default:
                return new Long[]{getState().getInVertexId(), getState().getOutVertexId()};
        }
    }

    @Override
    protected Property createNewProperty(final String key) {
        return new Neo4JEdgeProperty(this, key);
    }

}
