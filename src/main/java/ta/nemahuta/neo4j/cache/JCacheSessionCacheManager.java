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
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.Optional;

public class JCacheSessionCacheManager extends AbstractSessionCacheManager {

    public static final String CACHE_NAME_EDGE_GLOBAL = "edge-global";
    public static final String CACHE_NAME_VERTEX_GLOBAL = "vertex-global";

    protected final CacheManager cacheManager;
    private final Cache<Long, Neo4JEdgeState> globalEdgeCache;
    private final Cache<Long, Neo4JVertexState> globalVertexCache;

    private final Neo4JConfiguration configuration;
    private final Factory<? extends ExpiryPolicy> expiryFactory;


    public JCacheSessionCacheManager(@Nonnull final CachingProvider cachingProvider,
                                     @Nonnull final Neo4JConfiguration configuration) {
        this.cacheManager = Optional.ofNullable(configuration.getCacheConfiguration())
                .map(cacheConfig -> cachingProvider.getCacheManager(cacheConfig, getClass().getClassLoader()))
                .orElseGet(cachingProvider::getCacheManager);
        this.configuration = configuration;
        this.globalEdgeCache = getOrCreateCache(CACHE_NAME_EDGE_GLOBAL, Neo4JEdgeState.class);
        this.globalVertexCache = getOrCreateCache(CACHE_NAME_VERTEX_GLOBAL, Neo4JVertexState.class);
        this.expiryFactory = Optional.ofNullable(configuration.getCacheExpiryDuration())
                .map(AccessedExpiryPolicy::factoryOf)
                .orElseGet(EternalExpiryPolicy::factoryOf);
    }

    protected <E> Cache<Long, E> getOrCreateCache(final String globalEdgeCacheName, Class<E> elementCls) {
        return Optional.ofNullable(cacheManager.getCache(globalEdgeCacheName, Long.class, elementCls))
                .orElseGet(() -> cacheManager.createCache(globalEdgeCacheName, createCacheConfiguration()));
    }

    @Nonnull
    private <V> MutableConfiguration<Long, V> createCacheConfiguration() {
        final MutableConfiguration<Long, V> cacheConfig = new MutableConfiguration<>();
        cacheConfig.setExpiryPolicyFactory(expiryFactory);
        cacheConfig.setStatisticsEnabled(this.configuration.isCacheStatistics());
        cacheConfig.setManagementEnabled(this.configuration.isCacheStatistics());
        return cacheConfig;
    }


    @Override
    public SessionCache createSessionCache(final Object id) {
        return new DefaultSessionCache(
                new HierarchicalJCache<>(globalEdgeCache),
                createIdCache(globalKnownEdgeIds),
                new HierarchicalJCache<>(globalVertexCache),
                createIdCache(globalKnownVertexIds)
        );
    }

    @Override
    public void close() {
        globalVertexCache.close();
        globalEdgeCache.close();
        cacheManager.destroyCache(globalEdgeCache.getName());
        cacheManager.destroyCache(globalVertexCache.getName());
    }

}
