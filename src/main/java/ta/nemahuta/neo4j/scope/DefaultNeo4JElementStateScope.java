package ta.nemahuta.neo4j.scope;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.handler.Neo4JElementStateHandler;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

@RequiredArgsConstructor
@Slf4j
public class DefaultNeo4JElementStateScope<S extends Neo4JElementState, Q extends AbstractQueryBuilder> implements Neo4JElementStateScope<S, Q> {

    private final ReentrantReadWriteLock lockProvider = new ReentrantReadWriteLock();

    /**
     * the cache to be used to getAll loaded elements
     */
    @NonNull
    private final HierarchicalCache<Long, S> hierarchicalCache;

    @NonNull
    private final Neo4JElementStateHandler<S, Q> remoteElementHandler;

    @NonNull
    private final IdCache<Long> idCache;

    @Override
    public void update(final long id, @Nonnull final S newState) {
        if (idCache.isRemoved(id)) {
            throw new IllegalStateException("The element " + id + " has been deleted in the scope, cannot change to state: " + newState);
        }
        locked(ReadWriteLock::writeLock, () -> {
            final S state = getAll(Collections.singleton(id)).get(id);
            if (!Objects.equals(newState, state)) {
                log.trace("Change detected for element with id: {}", id);
                hierarchicalCache.put(id, newState);
                remoteElementHandler.update(id, state, newState);
            }
            return null;
        });
    }

    @Override
    public void delete(final long id) {
        if (idCache.isRemoved(id)) {
            return;
        }
        locked(ReadWriteLock::writeLock, () -> {
            log.trace("Deleting element with id temporary: {}", id);
            idCache.localRemoval(id);
            hierarchicalCache.remove(id);
            remoteElementHandler.delete(id);
            return null;
        });
    }

    @Override
    @Nonnull
    public long create(@Nonnull final S state) {
        return locked(ReadWriteLock::writeLock, () -> {
            final Long result = remoteElementHandler.create(state);
            hierarchicalCache.put(result, state);
            idCache.localCreation(result);
            return result;
        });
    }

    @Nullable
    @Override
    public S get(final long id) {
        return getAll(Collections.singleton(id)).get(id);
    }

    @Nullable
    @Override
    public Map<Long, S> getAll(@Nonnull final Collection<Long> ids) {
        return locked(ReadWriteLock::readLock, () -> {

            log.trace("Retrieving {} items", ids.isEmpty() ? "all" : ids.size());
            final Map<Long, S> results = new ConcurrentHashMap<>();
            final Set<Long> remainingIds = knownIdsForSelector(ids);

            log.trace("Trying {} elements from cache", remainingIds.size());
            putAllFromCache(remainingIds, results);
            log.trace("Got {} elements from cache", results.size());

            log.trace("Loading {} elements from the session.", remainingIds.size());
            loadFromSession(remainingIds, results);

            if (!remainingIds.isEmpty()) {
                if (knownIdsForSelector(remainingIds).size() == remainingIds.size()) {
                    throw new NoSuchElementException("Could not retrieve the items with the following ids: " +
                            String.join(",", Joiner.on(",").join(remainingIds)));
                } else {
                    log.debug("Elements which have been deleted by a parallel transaction: {}", remainingIds);
                }
            }

            return results;
        });
    }

    private Set<Long> knownIdsForSelector(@Nonnull final Collection<Long> ids) {
        if (ids.isEmpty()) {
            log.debug("Loading complete graph...");
            return idCache.getAll(remoteElementHandler::retrieveAllIds);
        } else {
            return idCache.filterExisting(ids, remoteElementHandler::retrieveAllIds);
        }
    }

    @Override
    public Map<Long, S> queryAndCache(@Nonnull final Function<Q, Q> query) {
        final Map<Long, S> queried = remoteElementHandler.query(query);
        for (final Map.Entry<Long, S> entry : queried.entrySet()) {
            hierarchicalCache.put(entry.getKey(), entry.getValue());
        }
        return ImmutableMap.copyOf(queried);
    }

    private void loadFromSession(final Set<Long> ids, final Map<Long, S> results) {
        if (ids.isEmpty()) {
            return;
        }
        remoteElementHandler.getAll(ids).forEach((k, v) -> {
            hierarchicalCache.put(k, v);
            results.put(k, v);
        });
        ids.removeAll(results.keySet());
    }

    private void putAllFromCache(final Collection<Long> ids, final Map<Long, S> results) {
        if (ids.isEmpty()) {
            return;
        }
        ids.parallelStream().forEach(k ->
                Optional.ofNullable(this.hierarchicalCache.get(k)).ifPresent(v -> {
                    results.put(k, v);
                })
        );
        ids.removeAll(results.keySet());
    }

    private <S> S locked(@Nonnull final Function<ReadWriteLock, Lock> lockFunction, @Nonnull final Supplier<S> fun) {
        final Lock lock = lockFunction.apply(this.lockProvider);
        lock.lock();
        try {
            return fun.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() {
        this.hierarchicalCache.commit();
        this.hierarchicalCache.removeFromParent(idCache.getRemoved());
        this.idCache.commit();
    }

    @Override
    public void rollback() {
        this.hierarchicalCache.clear();
        this.idCache.rollback();
    }

}
