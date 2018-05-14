package ta.nemahuta.neo4j.id;

import org.neo4j.driver.v1.types.Entity;

import javax.annotation.Nonnull;

/**
 * Interface for the adapter of {@link Neo4JElementId}s.
 *
 * @author Christian Heike
 */
public interface Neo4JElementIdAdapter<T> extends Neo4JElementIdGenerator<T> {

    /**
     * @return the property name used to store the identifier of an {@link Entity}.
     */
    @Nonnull
    String propertyName();

    /**
     * Retrieves the {@link Neo4JElementId} from an {@link Entity}.
     *
     * @param entity the source {@link Entity}
     * @return the {@link Neo4JElementId} retrieved from the entity
     */
    @Nonnull
    Neo4JElementId<T> retrieveId(@Nonnull Entity entity);

    /**
     * Converts the provided {@link Object} to a {@link Neo4JElementId}.
     *
     * @param id the object to be converted
     * @return the {@link Neo4JElementId} from the object
     */
    @Nonnull
    Neo4JElementId<T> convert(@Nonnull Object id);
}
