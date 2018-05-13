package ta.nemahuta.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import ta.nemahuta.neo4j.state.PropertyValue;

import java.util.Iterator;
import java.util.Optional;

public class Neo4JVertexProperty<T> extends Neo4JProperty<Neo4JVertex, T> implements VertexProperty<T> {


    public Neo4JVertexProperty(final Neo4JVertex parent, final String key) {
        super(parent, key);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Object id() {
        return parent.state.current(s -> Optional.ofNullable(s.state.properties.get(key)).map(PropertyValue::getId).get());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof VertexProperty && ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
