package ta.nemahuta.neo4j.cache;

import org.ehcache.Cache;

import javax.annotation.Nonnull;
import java.util.Set;

public interface HierarchicalCache<K, V> extends Cache<K, V> {

    /**
     * Commits the session cache to the global cache.
     */
    void commit();

    /**
     * Remove the keys from the parent cache due to deletion.
     *
     * @param keys the keys to be removed
     */
    void removeFromParent(@Nonnull Set<K> keys);
}
