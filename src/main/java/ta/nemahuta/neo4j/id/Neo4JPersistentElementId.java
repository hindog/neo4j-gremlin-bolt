package ta.nemahuta.neo4j.id;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;

/**
 * Denotes an {@link Neo4JElementId} which is already persisted.
 *
 * @param <T> the type of the actual identifier
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class Neo4JPersistentElementId<T> extends AbstractNeo4JElementId<T> {

    public Neo4JPersistentElementId(@Nonnull @NonNull final T id) {
        super(id);
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    @Nonnull
    public <R> Neo4JElementId<R> withId(@Nonnull @NonNull final R newValue) {
        return new Neo4JPersistentElementId<>(newValue);
    }

}
