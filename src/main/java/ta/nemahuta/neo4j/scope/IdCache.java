package ta.nemahuta.neo4j.scope;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ta.nemahuta.neo4j.session.RollbackAndCommit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    public Set<K> getAll(@Nonnull final Supplier<Set<K>> retrieval) {
        final Set<K> result = new HashSet<>(this.global.updateAndGet(source -> {
            if (source == null) {
                // Ids are not present, retrieve them
                return retrieval.get();
            } else {
                return source;
            }
        }));
        result.addAll(added);
        result.removeAll(removed);
        return result;
    }

    /**
     * Filter the source id's for known ids only.
     *
     * @param source    the source to be filtered
     * @param retrieval the retrieval function if the known ids are not loaded
     * @return the filtered collection as a set
     */
    @Nonnull
    public Set<K> filterExisting(@Nonnull final Collection<K> source, @Nonnull final Supplier<Set<K>> retrieval) {
        final Set<K> knownIds = getAll(retrieval);
        return source.stream()
                .filter(e -> !removed.contains(e) && (added.contains(e) || knownIds.contains(e)))
                .collect(Collectors.toSet());
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

    @Override
    public void commit() {
        global.getAndUpdate(source -> {
            if (source != null) {
                final Set<K> updated = new HashSet<>(source);
                updated.addAll(added);
                updated.removeAll(removed);
                return updated;
            } else {
                return source;
            }
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

    public ImmutableSet<K> getRemoved() {
        return ImmutableSet.copyOf(this.removed);
    }
}
