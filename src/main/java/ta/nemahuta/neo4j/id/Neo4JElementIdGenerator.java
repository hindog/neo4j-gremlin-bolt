package ta.nemahuta.neo4j.id;

import ta.nemahuta.neo4j.structure.Neo4JElement;

/**
 * An interface for the generator for a {@link Neo4JElementId}.
 *
 * @param <T> the type for {@link Neo4JElement#id()}
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Neo4JElementIdGenerator<T> {

    /**
     * Generates a new identifier value for a {@link Neo4JElement}.
     *
     * @return the new identifier value or <code>null</code> if provider does not support identifier generation.
     */
    Neo4JElementId<T> generate();

}
