package ta.nemahuta.neo4j.cache;

import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the {@link HierarchicalCache}.
 */
@Slf4j
public class HierarchicalJCache<K, V> implements HierarchicalCache<K, V> {

    /**
     * The global cache for all sessions
     */
    final Cache<K, V> parent;

    /**
     * The local cache for the session
     */
    final Map<K, Reference<V>> child;

    public HierarchicalJCache(@Nonnull final Cache<K, V> parent) {
        this(parent, new ConcurrentHashMap<>());
    }

    HierarchicalJCache(@Nonnull final Cache<K, V> parent, @Nonnull final Map<K, Reference<V>> child) {
        this.parent = parent;
        this.child = child;
    }

    @Override
    public void commit() {
        long counter = 0;
        for (final Map.Entry<K, Reference<V>> e : child.entrySet()) {
            final V value = e.getValue().get();
            if (value != null) {
                // The state is still in the cache
                parent.put(e.getKey(), value);
            } else {
                // The state has been removed from the cache, so we need to clear out the parent's state
                parent.remove(e.getKey());
            }
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
        final Reference<V> ref = child.get(key);
        if (ref != null) {
            return ref.get();
        } else {
            return parent.get(key);
        }
    }

    @Override
    public void put(final K key, final V value) {
        child.put(key, new SoftReference<>(value));
    }

    @Override
    public boolean remove(final K key) {
        return Optional.ofNullable(child.remove(key)).map(Reference::get).isPresent();
    }

    @Override
    public void clear() {
        log.debug("Clearing cache");
        child.clear();
    }

    @Override
    public Set<K> getKeys() {
        final Set<K> builder = new HashSet<>();
        for (final Cache.Entry<K, V> e : parent) {
            builder.add(e.getKey());
        }
        builder.addAll(child.keySet());
        return ImmutableSet.copyOf(builder);
    }

}
