package ta.nemahuta.neo4j.scope;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ta.nemahuta.neo4j.session.RollbackAndCommit;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@RequiredArgsConstructor
@Slf4j
public class IdCache<K> implements RollbackAndCommit {

    @NonNull
    private final AtomicReference<Set<K>> global;

    private final Set<K> added = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<K> removed = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @return the selector to load all identifiers, in case the identifiers are not known, returns {@link Collections#emptySet()}
     */
    @Nonnull
    public Set<K> getAllSelector() {
        return Optional.ofNullable(global.get())
                .map(glob -> {
                    final Set<K> result = new HashSet<>(glob);
                    result.addAll(added);
                    result.removeAll(removed);
                    return result;
                }).orElseGet(Collections::emptySet);
    }

    /**
     * Notify a creation of an element.
     *
     * @param id the id of the element
     */
    public void localCreation(final K id) {
        added.add(id);
        removed.remove(id);
    }

    /**
     * Notify a deletion of an element.
     *
     * @param id the id of the element
     */
    public void localRemoval(final K id) {
        if (!added.remove(id)) {
            removed.add(id);
        }
    }

    boolean isRemoved(final K id) {
        return removed.contains(id);
    }

    /**
     * Notify a complete load of the ids.
     *
     * @param loaded the loaded ids from the remote.
     */
    public void completeLoad(final Set<K> loaded) {
        modifyGlobal(glob -> loaded.stream().filter(id -> !added.contains(id) && !removed.contains(id)).forEach(glob::add));
    }

    @Override
    public void commit() {
        modifyGlobal(glob -> {
            glob.addAll(added);
            glob.removeAll(removed);
        });
        resetLocalScope();
    }


    @Override
    public void rollback() {
        resetLocalScope();
    }

    protected void resetLocalScope() {
        added.clear();
        removed.clear();
    }

    protected void modifyGlobal(@Nonnull final Consumer<Set<K>> consumer) {
        global.getAndUpdate(glob -> {
            consumer.accept(glob);
            return glob;
        });
    }

    public ImmutableSet<K> getRemoved() {
        return ImmutableSet.copyOf(this.removed);
    }
}
