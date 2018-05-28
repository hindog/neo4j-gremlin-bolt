package ta.nemahuta.neo4j.cache;

import org.ehcache.Cache;

import java.util.Iterator;

public interface HierarchicalCache<K, V> extends Cache<K, V> {

    /**
     * Commits the session cache to the global cache.
     */
    void commit();

    /**
     * @return the {@link Iterator} which returns the child cache entries only
     */
    Iterator<Entry<K, V>> childIterator();

}
