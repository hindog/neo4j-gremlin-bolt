package ta.nemahuta.neo4j.id;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;

/**
 * Denotes a transient element identifier, which is only temporary.
 *
 * @param <T> the type of the actual identifier.
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class Neo4JTransientElementId<T> extends AbstractNeo4JElementId<T> {

    public Neo4JTransientElementId(final T id) {
        super(id);
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    @Nonnull
    public <R> Neo4JElementId<R> withId(@NonNull @Nonnull final R newValue) {
        return new Neo4JTransientElementId<>(newValue);
    }
}
