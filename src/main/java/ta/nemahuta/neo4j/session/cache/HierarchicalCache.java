package ta.nemahuta.neo4j.session.cache;

import org.ehcache.Cache;

public interface HierarchicalCache<K, V> extends Cache<K, V> {

    /**
     * Commits the session cache to the global cache.
     */
    void commit();

}
