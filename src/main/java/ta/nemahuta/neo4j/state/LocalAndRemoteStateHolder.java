package ta.nemahuta.neo4j.state;

import lombok.NonNull;
import lombok.ToString;
import ta.nemahuta.neo4j.async.AsyncAccess;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A tuple of {@link StateHolder}s which hold the committed and current state, so they can be committed and rolled back.
 *
 * @param <S> the type of the state
 * @author Christian Heike (christian.heike@icloud.com)
 */
@ToString
public class LocalAndRemoteStateHolder<S> {

    private final AsyncAccess<StateHolder<S>> committedState;
    private final AsyncAccess<StateHolder<S>> currentState;

    /**
     * Create a new state holder with an initial state.
     *
     * @param initialState the initial state
     */
    public LocalAndRemoteStateHolder(@Nonnull @NonNull final StateHolder<S> initialState) {
        // Note: in case the initial state is TRANSIENT, the committed state is actually DISCARDED
        final StateHolder<S> rollbackState = SyncState.TRANSIENT.equals(initialState.getSyncState()) ?
                new StateHolder<>(SyncState.DISCARDED, initialState.getState()) :
                initialState;
        this.committedState = new AsyncAccess<>(rollbackState);
        this.currentState = new AsyncAccess<>(initialState);
    }

    /**
     * Modify the current state by an update function.
     *
     * @param update the update function to be used
     * @return true if the updated modified the current state
     */
    public boolean modify(@Nonnull @NonNull final Function<S, S> update) {
        boolean[] result = new boolean[]{false};
        this.currentState.update(s -> {
            final S newState = update.apply(s.getState());
            final StateHolder<S> newStateHolder = s.modify(newState);
            result[0] = newStateHolder == s;
            return newStateHolder;
        });
        return result[0];
    }

    /**
     * Mark the current state as deleted.
     */
    public void delete() {
        this.currentState.update(StateHolder::delete);
    }

    /**
     * Commits the state using a function uses both state holders to create a new state.
     * Additionally marks the current state as in sync.
     *
     * @param stateConsumer the consumer for the commit
     */
    public void commit(@Nonnull @NonNull final BiFunction<StateHolder<S>, StateHolder<S>, S> stateConsumer) {
        // Lock the committed and current state for the update
        this.committedState.update(cm ->
                currentState.update(cur -> {
                    // Provide the states to the consumer to handle the commit
                    final S committed = stateConsumer.apply(cm, cur);
                    // Update the current state by marking it synchronized
                    return cur.synced(committed);
                })
        );
    }

    /**
     * Rolls back the current state to the last known committed one.
     */
    public void rollback() {
        // Lock the committed and current state for the update
        currentState.update(cur -> committedState.get(Function.identity()));
    }

    /**
     * Queries the current state.
     *
     * @param function the function for the query
     * @param <R>      the result type
     * @return the result of the function
     */
    @Nullable
    public <R> R current(@Nonnull @NonNull final Function<S, R> function) {
        return currentState.get(s -> function.apply(s.getState()));
    }

    /**
     * @return the current synchronisation state
     */
    @Nonnull
    public SyncState getCurrentSyncState() {
        return Optional.ofNullable(currentState.get(s -> s.getSyncState())).orElse(SyncState.DISCARDED);
    }

}
