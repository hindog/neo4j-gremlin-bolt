package ta.nemahuta.neo4j.id;

import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Denotes an {@link Neo4JElementId} which is already persisted.
 *
 * @param <T> the type of the actual identifier
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JPersistentElementId<T> extends AbstractNeo4JElementId<T> {

    public Neo4JPersistentElementId(@Nonnull @NonNull final T id) {
        super(id, true);
    }

}
