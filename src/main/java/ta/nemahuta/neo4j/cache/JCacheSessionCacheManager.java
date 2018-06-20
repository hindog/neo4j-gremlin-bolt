package ta.nemahuta.neo4j.cache;

import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.scope.IdCache;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class JCacheSessionCacheManager implements SessionCacheManager {

    public static final String CACHE_NAME_EDGE_GLOBAL = "edge-global";
    public static final String CACHE_NAME_VERTEX_GLOBAL = "vertex-global";

    protected final CacheManager cacheManager;
    protected final Cache<Long, Neo4JEdgeState> globalEdgeCache;
    protected final AtomicReference<Set<Long>> globalKnownEdgeIds = new AtomicReference<>(new HashSet<>());
    protected final Cache<Long, Neo4JVertexState> globalVertexCache;
    protected final AtomicReference<Set<Long>> globalKnownVertexIds = new AtomicReference<>(new HashSet<>());
    private final Neo4JConfiguration configuration;
    private final Factory<? extends ExpiryPolicy> expiryFactory;

    private final String globalEdgeCacheName;
    private final String globalVertexCacheName;

    public JCacheSessionCacheManager(@Nonnull final CacheManager cacheManager,
                                     @Nonnull final Neo4JConfiguration configuration) {
        this.cacheManager = cacheManager;
        this.configuration = configuration;
        final String sessionName = configuration.getHostname() + ":" + configuration.getPort();
        this.globalEdgeCacheName = CACHE_NAME_EDGE_GLOBAL + "-" + sessionName;
        this.globalVertexCacheName = CACHE_NAME_VERTEX_GLOBAL + "-" + sessionName;
        this.globalEdgeCache = Optional.ofNullable(cacheManager.getCache(globalEdgeCacheName, Long.class, Neo4JEdgeState.class))
                .orElseGet(() -> createEdgeCache(globalEdgeCacheName));
        this.globalVertexCache = Optional.ofNullable(cacheManager.getCache(globalVertexCacheName, Long.class, Neo4JVertexState.class))
                .orElseGet(() -> createVertexCache(globalVertexCacheName));
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
        return new DefaultSessionCache(
                new HierarchicalJCache<>(globalEdgeCache),
                new IdCache<>(globalKnownEdgeIds),
                new HierarchicalJCache<>(globalVertexCache),
                new IdCache<>(globalKnownVertexIds)
        );
    }

    @Override
    public void close() {
        globalVertexCache.close();
        globalEdgeCache.close();
        cacheManager.destroyCache(globalEdgeCacheName);
        cacheManager.destroyCache(globalVertexCacheName);
    }

}
