package ta.nemahuta.neo4j.structure;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link Property} for {@link org.apache.tinkerpop.gremlin.structure.Edge}s for neo4j.
 *
 * @param <T> the type of the property value
 */
public class Neo4JEdgeProperty<T> extends Neo4JProperty<Neo4JEdge, T> {

    public Neo4JEdgeProperty(@NonNull @Nonnull final Neo4JEdge parent,
                             @NonNull @Nonnull final String key) {
        super(parent, key);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Property && ElementHelper.areEqual(this, object);
    }
}
