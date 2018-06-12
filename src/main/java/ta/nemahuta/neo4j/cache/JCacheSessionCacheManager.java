package ta.nemahuta.neo4j.cache;

import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;

public class JCacheSessionCacheManager implements SessionCacheManager {

    public static final String CACHE_NAME_EDGE_GLOBAL = "edge-global";
    public static final String CACHE_NAME_VERTEX_GLOBAL = "vertex-global";

    protected final CacheManager cacheManager;
    protected final Cache<Long, Neo4JEdgeState> globalEdgeCache;
    protected final Cache<Long, Neo4JVertexState> globalVertexCache;
    private final Neo4JConfiguration configuration;
    private final Factory<? extends ExpiryPolicy> expiryFactory;

    private final String globalEdgeCacheName;
    private final String globalVertexCacheName;

    public JCacheSessionCacheManager(@Nonnull final CacheManager cacheManager,
                                     @Nonnull final Neo4JConfiguration configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        this.globalEdgeCacheName = CACHE_NAME_EDGE_GLOBAL + "-" + configuration.hashCode();
        this.globalVertexCacheName = CACHE_NAME_VERTEX_GLOBAL + "-" + configuration.hashCode();
        this.globalEdgeCache = createEdgeCache(globalEdgeCacheName);
        this.globalVertexCache = createVertexCache(globalVertexCacheName);
        this.expiryFactory = () -> new AccessedExpiryPolicy(configuration.getCacheExpiryDuration());
    }

    private Cache<Long, Neo4JVertexState> createVertexCache(final String name) {
        final MutableConfiguration<Long, Neo4JVertexState> cacheConfig = new MutableConfiguration<>();
        cacheConfig.setExpiryPolicyFactory(expiryFactory);
        cacheConfig.setStatisticsEnabled(this.configuration.isCacheStatistics());
        return cacheManager.createCache(name, cacheConfig);
    }

    protected Cache<Long, Neo4JEdgeState> createEdgeCache(final String name) {
        final MutableConfiguration<Long, Neo4JEdgeState> cacheConfig = new MutableConfiguration<>();
        cacheConfig.setExpiryPolicyFactory(expiryFactory);
        cacheConfig.setStatisticsEnabled(this.configuration.isCacheStatistics());
        return cacheManager.createCache(name, cacheConfig);
    }

    @Override
    public SessionCache createSessionCache(final Object id) {
        final String vertexCacheName = "vertex-session-" + id;
        final String edgeCacheName = "edge-session-" + id;
        final Cache<Long, Neo4JVertexState> sessionVertexCache = createVertexCache(vertexCacheName);
        final Cache<Long, Neo4JEdgeState> sessionEdgeCache = createEdgeCache(edgeCacheName);
        return new DefaultSessionCache(
                new HierarchicalJCache<>(globalEdgeCache, sessionEdgeCache),
                new HierarchicalJCache<>(globalVertexCache, sessionVertexCache)) {

            @Override
            public void close() {
                sessionVertexCache.close();
                sessionEdgeCache.close();
                cacheManager.destroyCache(vertexCacheName);
                cacheManager.destroyCache(edgeCacheName);
            }
        };
    }

    @Override
    public void close() {
        globalVertexCache.close();
        globalEdgeCache.close();
        cacheManager.destroyCache(globalEdgeCacheName);
        cacheManager.destroyCache(globalVertexCacheName);
        cacheManager.close();
    }
}
