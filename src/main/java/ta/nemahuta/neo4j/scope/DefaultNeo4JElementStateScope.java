package ta.nemahuta.neo4j.scope;

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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final Neo4JElementStateHandler remoteElementHandler;

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
            log.trace("Retrieving {} items", ids.size());
            final Map<Long, S> results = new ConcurrentHashMap<>();
            final Collection<Long> idsToBeRetrieved = ids.isEmpty() ? idCache.getAllSelector() : ids;
            if (idsToBeRetrieved.isEmpty()) {
                log.debug("Loading complete graph.");
            }
            if (!idsToBeRetrieved.isEmpty()) {
                log.trace("Putting {} elements from cache", idsToBeRetrieved.size());
                putAllFromCache(idsToBeRetrieved, results);
            }
            final int fromCacheSize = results.size();
            final Set<Long> idsToBeLoaded = idsToBeRetrieved.stream().filter(id -> !results.containsKey(id)).collect(Collectors.toSet());
            log.trace("Found {} elements to be in scope already.", results.size());
            if (!idsToBeLoaded.isEmpty() || idsToBeRetrieved.isEmpty()) {
                log.trace("Loading the other {} elements.", idsToBeLoaded.size());
                loadFromSession(idsToBeLoaded, results);
            }
            if (!idsToBeRetrieved.isEmpty() && idsToBeRetrieved.size() != results.size()) {
                log.warn("Could not retrieve all {} elements, got {} from cache, and {} from the session.", idsToBeLoaded.size(), fromCacheSize, results.size() - fromCacheSize);
            }
            // Now we load all the remaining elements and put them to the cache and in the results
            return results;
        });
    }

    @Override
    public Map<Long, S> queryAndCache(@Nonnull final Function<Q, Q> query) {
        final Map<Long, S> queried = remoteElementHandler.query(query);
        for (final Map.Entry<Long, S> entry : queried.entrySet()) {
            hierarchicalCache.put(entry.getKey(), entry.getValue());
        }
        return ImmutableMap.copyOf(queried);
    }

    private void loadFromSession(final Set<Long> selectorOrEmpty, final Map<Long, S> results) {
        final Map<Long, S> loaded = remoteElementHandler.getAll(selectorOrEmpty);
        log.trace("Loaded {} items", loaded.size());
        for (final Map.Entry<Long, S> entry : loaded.entrySet()) {
            if (!idCache.isRemoved(entry.getKey())) {
                results.put(entry.getKey(), entry.getValue());
                hierarchicalCache.put(entry.getKey(), entry.getValue());
            }
        }
        if (selectorOrEmpty.isEmpty()) {
            idCache.completeLoad(results.keySet());
        }
    }

    private void putAllFromCache(final Collection<Long> selectorOrEmpty, final Map<Long, S> results) {
        final long beforeSize = results.size();
        final Stream<Long> selector = (
                !selectorOrEmpty.isEmpty() ?
                        selectorOrEmpty.stream() :
                        hierarchicalCache.getKeys().filter(key -> !idCache.isRemoved(key))
        ).distinct();

        selector.forEach(key -> {
            final S value = this.hierarchicalCache.get(key);
            if (value != null) {
                results.put(key, value);
            }
        });
        log.trace("Put {} elements from cache", results.size() - beforeSize);
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
