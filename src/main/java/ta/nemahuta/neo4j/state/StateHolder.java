package ta.nemahuta.neo4j.state;

import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A state holder which holds a certain value and a {@link SyncState} denoting the remote synchronization.
 *
 * @param <S> the type of the value to be held
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class StateHolder<S> {

    /**
     * the synchronisation state
     */
    @Getter(onMethod = @__(@Nonnull))
    private final SyncState syncState;

    /**
     * the actual state value
     */
    @Getter(onMethod = @__(@Nonnull))
    private final S state;

    /**
     * Create a new state holder from the provided parameters.
     *
     * @param syncState the remote synchronization state
     * @param state     the value
     */
    public StateHolder(@Nonnull @NonNull final SyncState syncState,
                       @Nonnull @NonNull final S state) {
        this.syncState = syncState;
        this.state = state;
    }

    /**
     * Construct a new state holder using the new value.
     *
     * @param newState the new value
     * @return the new state holder
     */
    @Nonnull
    public StateHolder<S> modify(@Nonnull @NonNull final S newState) {
        if (Objects.equals(this.getState(), newState)) {
            return this;
        }
        return new StateHolder<>(syncState.asModified(), newState);
    }

    /**
     * @return the state with the same value but marked deleted
     */
    @Nonnull
    public StateHolder<S> delete() {
        return new StateHolder(syncState.asDeleted(), state);
    }

    /**
     * Construct a new state holder using the new state marking it {@link SyncState#SYNCHRONOUS}.
     *
     * @param state the state
     * @return the new state holder
     */
    public StateHolder<S> synced(final S state) {
        return new StateHolder<>(this.syncState.asSynchronized(), state);
    }

}
