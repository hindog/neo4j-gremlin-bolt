package ta.nemahuta.neo4j.cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public interface GraphFactoryCache<K, V> {

    /**
     * Put an element to the cache.
     *
     * @param key   the key for the element
     * @param value the value to be cached
     * @return the replaced element
     */
    V put(@Nonnull K key, @Nonnull V value);

    /**
     * Get a value from the cache.
     *
     * @param key the key of the element
     * @return the value or {@code null} if the key is not cached
     */
    @Nullable
    V get(@Nonnull K key);

    /**
     * @return all keys of the elements in the cache
     */
    @Nonnull
    Set<K> getKeys();

    /**
     * Removes all elements with the provided keys from the cache
     *
     * @param keys the keys to be removed
     */
    @Nonnull
    void removeAll(Iterable<K> keys);

}
