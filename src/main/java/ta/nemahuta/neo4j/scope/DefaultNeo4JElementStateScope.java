package ta.nemahuta.neo4j.scope;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ehcache.Cache;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.handler.Neo4JElementStateHandler;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
@Slf4j
public class DefaultNeo4JElementStateScope<S extends Neo4JElementState> implements Neo4JElementStateScope<S> {

    private final ReentrantReadWriteLock lockProvider = new ReentrantReadWriteLock();

    /**
     * the cache to be used to getAll loaded elements
     */
    @NonNull
    private final HierarchicalCache<Long, S> hierarchicalCache;

    @NonNull
    private final Neo4JElementStateHandler remoteElementHandler;

    private final Set<Long> deleted = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final AtomicBoolean completelyLoaded = new AtomicBoolean(false);

    @Override
    public void update(final long id,
                       @Nonnull final S newState) {
        locked(ReadWriteLock::writeLock, () -> {
            final S state = getAll(Collections.singleton(id)).get(id);
            if (!Objects.equals(newState, state)) {
                log.debug("Change detected for element with id: {}", id);
                hierarchicalCache.put(id, newState);
                remoteElementHandler.update(id, state, newState);
            }
            return null;
        });
    }

    @Override
    public void delete(final long id) {
        locked(ReadWriteLock::writeLock, () -> {
            log.debug("Removing element with id from the queue: {}", id);
            deleted.add(id);
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
            return result;
        });
    }

    @Nullable
    @Override
    public S get(final long id) {
        return locked(ReadWriteLock::readLock, () -> getAll(Collections.singleton(id)).get(id));
    }

    @Nullable
    @Override
    public Map<Long, S> getAll(@Nonnull final Collection<Long> ids) {
        return locked(ReadWriteLock::readLock, () -> {
            log.debug("Retrieving {} items", ids.size());
            final Map<Long, S> results = new ConcurrentHashMap<>();
            if (ids.isEmpty()) {
                // Mark all ids to be loaded
                if (completelyLoaded.getAndSet(true)) {
                    log.trace("Scope is populated, using cache items only.");
                    // if all ids have been loaded at this point, we take them from the cache completely
                    putAllFromCache(ImmutableSet.of(), results);
                } else {
                    log.trace("Scope is not populated yet, loading all items.");
                    // in the other case we must load each and every entry from the scope
                    loadFromSession(ImmutableSet.of(), results);
                }
            } else {
                putAllFromCache(ids, results);
                final Set<Long> idsToBeLoaded = ids.stream().filter(id -> !results.containsKey(id)).collect(Collectors.toSet());
                log.trace("Found {} elements to be in scope already.", results.size());
                if (!idsToBeLoaded.isEmpty()) {
                    log.trace("Loading the other {} elements.", idsToBeLoaded.size());
                    loadFromSession(idsToBeLoaded, results);
                }
            }
            // Now we load all the remaining elements and put them to the cache and in the results
            return results;
        });
    }

    private void loadFromSession(final Set<Long> selectorOrEmpty, final Map<Long, S> results) {
        final Map<Long, S> loaded = remoteElementHandler.getAll(selectorOrEmpty);
        log.trace("Loaded {} items", loaded.size());
        for (final Map.Entry<Long, S> entry : loaded.entrySet()) {
            if (!deleted.contains(entry.getKey())) {
                results.put(entry.getKey(), entry.getValue());
                hierarchicalCache.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void putAllFromCache(final Collection<Long> selectorOrEmpty, final Map<Long, S> results) {
        final long beforeSize = results.size();
        final Stream<Long> selector = (!selectorOrEmpty.isEmpty() ? selectorOrEmpty.stream() : cacheKeys())
                .filter(key -> !deleted.contains(key))
                .distinct();

        selector.forEach(key -> {
            final S value = this.hierarchicalCache.get(key);
            if (value != null) {
                results.put(key, value);
            }
        });
        log.trace("Put {} elements from cache", results.size() - beforeSize);
    }

    @Nonnull
    private Stream<Long> cacheKeys() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.hierarchicalCache.iterator(), Spliterator.ORDERED), true)
                .map(Cache.Entry::getKey);
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
        this.hierarchicalCache.removeFromParent(deleted);
    }

    @Override
    public void rollback() {
        this.hierarchicalCache.clear();
        this.deleted.clear();
    }
}
