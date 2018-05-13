package ta.nemahuta.neo4j.id;

import ta.nemahuta.neo4j.structure.Neo4JElement;

import javax.annotation.Nonnull;

/**
 * A class wrapping an id for a {@link Neo4JElement}.
 *
 * @param <T> the type of the id
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JElementId<T> {

    /**
     * @return the real id for the element
     */
    T getId();

    /**
     * @return true if the id is actually persistent
     */
    boolean isRemote();

    /**
     * Create a new element of the same type using a new value.
     *
     * @param newValue the new value
     * @param <R>      the result identifier type
     * @return the new element id
     */
    @Nonnull
    <R> Neo4JElementId<R> withId(@Nonnull R newValue);

}
