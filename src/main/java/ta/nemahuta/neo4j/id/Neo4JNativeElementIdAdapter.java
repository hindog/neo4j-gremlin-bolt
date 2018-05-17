package ta.nemahuta.neo4j.id;

import lombok.NonNull;
import org.neo4j.driver.v1.types.Entity;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * {@link AbstractNeo4JElementIdAdapter} implementation based which will used generated ids until the elements have been persisted.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JNativeElementIdAdapter extends AbstractNeo4JElementIdAdapter {

    public static final String PROPERTY_NAME = "id";

    /**
     * The id generator for the transient ids
     */
    private final Neo4JTransientElementIdGenerator idGenerator = new Neo4JTransientElementIdGenerator();

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    @Override
    public Optional<String> propertyName() {
        return Optional.empty();
    }

    /**
     * Gets the identifier value from a neo4j {@link Entity}.
     *
     * @param entity The neo4j {@link Entity}.
     * @return The neo4j {@link Entity} identifier.
     */
    @Override
    @Nonnull
    public Neo4JElementId<Long> retrieveId(@Nonnull @NonNull final Entity entity) {
        return new Neo4JPersistentElementId<>(entity.id());
    }

    /**
     * Generates a new identifier value. This {@link Neo4JElementIdAdapter} will fetch a pool of identifiers
     * from a Neo4J database Node.
     *
     * @return A unique identifier within the database sequence generator.
     */
    @Override
    @Nonnull
    public Neo4JElementId<Long> generate() {
        return idGenerator.generate();
    }

}
