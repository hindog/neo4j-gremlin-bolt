package ta.nemahuta.neo4j.cache;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Cache;
import java.util.Set;

public interface HierarchicalCache<K, V> {

    /**
     * Get an element from the cache.
     *
     * @param key the key
     * @return the value for the key
     */
    @Nullable
    V get(@Nonnull K key);

    /**
     * Put an element to the cache.
     *
     * @param key   the key of the element
     * @param value the value to put to the cache
     */
    void put(@Nonnull K key, @Nonnull V value);

    /**
     * Remove an element from the cache.
     *
     * @param key the key of the element
     * @return {@code true} if an element was removed
     */
    boolean remove(@Nonnull K key);

    /**
     * Remove the keys from the parent cache due to deletion.
     *
     * @param keys the keys to be removed
     */
    void removeFromParent(@Nonnull Set<K> keys);

    /**
     * Commits the session cache to the global cache.
     */
    void commit();

    /**
     * Clear the cache.
     */
    void clear();

    /**
     * @return the keys of the elements the cache has encountered, regardless of their cache status
     */
    Set<K> getKeys();
}
