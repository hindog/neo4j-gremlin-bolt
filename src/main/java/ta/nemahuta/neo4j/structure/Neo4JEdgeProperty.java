package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * The {@link Neo4JProperty} accessor for a property on an {@link Neo4JEdge}.
 *
 * @param <T> the type of the property value
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JEdgeProperty<T> extends Neo4JProperty<Neo4JEdge, T> implements Property<T> {

    public Neo4JEdgeProperty(final Neo4JEdge edge, String name) {
        super(edge, name);
    }

}
