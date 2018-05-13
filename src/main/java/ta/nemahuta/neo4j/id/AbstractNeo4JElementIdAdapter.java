package ta.nemahuta.neo4j.id;

import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Abstract implementation of {@link Neo4JElementIdAdapter} which already implements {@link #convert(Object)}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public abstract class AbstractNeo4JElementIdAdapter implements Neo4JElementIdAdapter<Long> {

    /**
     * Process the given identifier converting it to the correct type if necessary.
     *
     * @param id The {@link org.apache.tinkerpop.gremlin.structure.Element} identifier.
     * @return The {@link org.apache.tinkerpop.gremlin.structure.Element} identifier converted to the correct type if necessary.
     */
    @Override
    public Neo4JElementId<Long> convert(@Nonnull @NonNull final Object id) {
        if (id instanceof Neo4JElementId &&
                ((Neo4JElementId<?>) id).getId() instanceof Long) {
            return (Neo4JElementId<Long>) id;
        }
        return convertId(id)
                .map(Neo4JPersistentElementId::new)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass())));
    }

    /**
     * Converts the provided {@link Object} into a {@link Long} value, which can be used as a wrapped identifier.
     *
     * @param id the object
     * @return the optional containing the extracted value or {@link Optional#empty()}
     */
    @Nonnull
    protected Optional<Long> convertId(@Nonnull @NonNull Object id) {
        // check for Long
        if (id instanceof Neo4JElementId) {
            id = ((Neo4JElementId<?>) id).getId();
        }
        if (id instanceof Long) {
            return Optional.of((Long) id);
            // check for numeric types
        } else if (id instanceof Number) {
            return Optional.of(((Number) id).longValue());
            // check for string
        } else if (id instanceof String) {
            return Optional.of(Long.valueOf((String) id));
        } else {
            return Optional.empty();
        }
    }

}
