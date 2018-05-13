package ta.nemahuta.neo4j.state;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import javax.annotation.Nonnull;

/**
 * Adapter enumeration for property types.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public enum PropertyCardinality {
    SINGLE,
    LIST,
    SET;

    @Nonnull
    public static PropertyCardinality from(@NonNull @Nonnull final VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case list:
                return PropertyCardinality.LIST;
            case set:
                return PropertyCardinality.SET;
            case single:
                return PropertyCardinality.SINGLE;
            default:
                throw new IllegalArgumentException("Cannot convert cardinality: " + cardinality);
        }
    }

}
