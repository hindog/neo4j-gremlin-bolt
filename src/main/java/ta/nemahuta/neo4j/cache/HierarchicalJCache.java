package ta.nemahuta.neo4j.cache;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import java.util.*;

/**
 * Default implementation of the {@link HierarchicalCache}.
 */
@RequiredArgsConstructor
@Slf4j
public class HierarchicalJCache<K, V> implements HierarchicalCache<K, V> {

    /**
     * The global cache for all sessions
     */
    @NonNull
    Cache<K, V> parent;

    /**
     * The local cache for the session.
     */
    @NonNull
    Cache<K, V> child;

    @Override
    public void commit() {
        long counter = 0;
        for (final Cache.Entry<K, V> e : child) {
            parent.put(e.getKey(), e.getValue());
            child.remove(e.getKey());
            counter++;
        }
        log.debug("Committed {} elements in cache", counter);
    }

    @Override
    public void removeFromParent(@Nonnull final Set<K> keys) {
        log.debug("Removing {} elements from parent.", keys.size());
        parent.removeAll(keys);
    }

    @Override
    public V get(final K key) {
        return Optional.ofNullable(child.get(key)).orElseGet(() -> parent.get(key));
    }

    @Override
    public void put(final K key, final V value) {
        child.put(key, value);
    }

    @Override
    public boolean remove(final K key) {
        return child.remove(key);
    }

    @Override
    public void clear() {
        log.debug("Clearing cache");
        child.clear();
    }

    @Override
    public Iterator<Cache.Entry<K, V>> iterator() {
        final Map<K, Cache.Entry<K, V>> result = new HashMap<>();
        parent.iterator().forEachRemaining(e -> result.put(e.getKey(), e));
        child.iterator().forEachRemaining(e -> result.put(e.getKey(), e));
        return result.values().iterator();
    }
}
