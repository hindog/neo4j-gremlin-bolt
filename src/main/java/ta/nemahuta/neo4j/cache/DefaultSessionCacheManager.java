package ta.nemahuta.neo4j.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import java.time.Duration;

public class DefaultSessionCacheManager implements SessionCacheManager {

    public static final String CACHE_NAME_EDGE_GLOBAL = "edge-global";
    public static final String CACHE_NAME_VERTEX_GLOBAL = "vertex-global";

    protected final CacheManager cacheManager;
    protected final Cache<Long, Neo4JEdgeState> globalEdgeCache;
    protected final Cache<Long, Neo4JVertexState> globalVertexCache;

    public DefaultSessionCacheManager(@Nonnull final CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.globalEdgeCache = createEdgeCache(CACHE_NAME_EDGE_GLOBAL);
        this.globalVertexCache = createVertexCache(CACHE_NAME_VERTEX_GLOBAL);
    }

    private Cache<Long, Neo4JVertexState> createVertexCache(final String name) {
        return cacheManager.createCache(name,
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Neo4JVertexState.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(200, MemoryUnit.MB))
                        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(30))));
    }

    protected Cache<Long, Neo4JEdgeState> createEdgeCache(final String name) {
        return cacheManager.createCache(name,
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Neo4JEdgeState.class,
                        ResourcePoolsBuilder.newResourcePoolsBuilder().heap(200, MemoryUnit.MB))
                        .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(30))));
    }

    @Override
    public SessionCache createSessionCache(final Object id) {
        final String vertexCacheName = "vertex-session-" + id;
        final String edgeCacheName = "edge-session-" + id;
        final Cache<Long, Neo4JVertexState> sessionVertexCache = createVertexCache(vertexCacheName);
        final Cache<Long, Neo4JEdgeState> sessionEdgeCache = createEdgeCache(edgeCacheName);
        return new DefaultSessionCache(
                new DefaultHierarchicalCache<>(globalEdgeCache, sessionEdgeCache),
                new DefaultHierarchicalCache<>(globalVertexCache, sessionVertexCache)) {

            @Override
            public void close() {
                cacheManager.removeCache(vertexCacheName);
                cacheManager.removeCache(edgeCacheName);
            }
        };
    }

    @Override
    public void close() {
        cacheManager.removeCache(CACHE_NAME_EDGE_GLOBAL);
        cacheManager.removeCache(CACHE_NAME_VERTEX_GLOBAL);
        cacheManager.close();
    }
}
