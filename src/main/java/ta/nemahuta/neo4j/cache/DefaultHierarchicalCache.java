package ta.nemahuta.neo4j.cache;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.ehcache.Cache;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

import java.util.*;

/**
 * Default implementation of the {@link HierarchicalCache}.
 */
@RequiredArgsConstructor
public class DefaultHierarchicalCache<K, V> implements HierarchicalCache<K, V> {

    /**
     * The global cache for all sessions
     */
    @NonNull
    private Cache<K, V> parent;

    /**
     * The local cache for the session.
     */
    @NonNull
    private Cache<K, V> child;

    @Override
    public void commit() {
        child.forEach(e -> {
            parent.put(e.getKey(), e.getValue());
            child.remove(e.getKey());
        });
    }

    @Override
    public V get(final K key) throws CacheLoadingException {
        return Optional.ofNullable(child.get(key)).orElseGet(() -> parent.get(key));
    }

    @Override
    public void put(final K key, final V value) throws CacheWritingException {
        child.put(key, value);
    }

    @Override
    public boolean containsKey(final K key) {
        return child.containsKey(key) || parent.containsKey(key);
    }

    @Override
    public void remove(final K key) throws CacheWritingException {
        child.remove(key);
    }

    @Override
    public Map<K, V> getAll(final Set<? extends K> keys) throws BulkCacheLoadingException {
        final Map<K, V> result = new HashMap<>(parent.getAll(keys));
        result.putAll(child.getAll(keys));
        return result;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
        child.putAll(entries);
    }

    @Override
    public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
        child.removeAll(keys);
    }

    @Override
    public void clear() {
        child.clear();
    }

    @Override
    public V putIfAbsent(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        final V global = parent.get(key);
        return global != null ? global : child.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final K key, final V value) throws CacheWritingException {
        return child.remove(key, value);
    }

    @Override
    public V replace(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        return child.replace(key, value);
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) throws CacheLoadingException, CacheWritingException {
        return child.replace(key, oldValue, newValue);
    }

    @Override
    public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
        return child.getRuntimeConfiguration();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        final Map<K, Entry<K, V>> result = new HashMap<>();
        parent.iterator().forEachRemaining(e -> result.put(e.getKey(), e));
        child.iterator().forEachRemaining(e -> result.put(e.getKey(), e));
        return result.values().iterator();
    }
}
