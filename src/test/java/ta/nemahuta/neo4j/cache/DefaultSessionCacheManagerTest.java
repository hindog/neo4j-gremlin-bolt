package ta.nemahuta.neo4j.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultSessionCacheManagerTest {

    @Mock
    private CacheManager cacheManager;

    @Mock
    private Cache<Long, Neo4JVertexState> sessionVertexCache, globalVertexCache;
    @Mock
    private Cache<Long, Neo4JEdgeState> sessionEdgeCache, globalEdgeCache;

    private SessionCacheManager sut;

    @BeforeEach
    void stubGlobalCreationAndCreateSut() {
        when(cacheManager.createCache(eq("vertex-global"), any(CacheConfigurationBuilder.class))).thenReturn(globalVertexCache);
        when(cacheManager.createCache(eq("edge-global"), any(CacheConfigurationBuilder.class))).thenReturn(globalEdgeCache);
        this.sut = new DefaultSessionCacheManager(cacheManager);
    }


    @Test
    void createSessionCache() {
        // setup: 'stub the creation of the session caches'
        when(cacheManager.createCache(eq("vertex-session-a"), any(CacheConfigurationBuilder.class))).thenReturn(sessionVertexCache);
        when(cacheManager.createCache(eq("edge-session-a"), any(CacheConfigurationBuilder.class))).thenReturn(sessionEdgeCache);
        // when: 'creating a session cache'
        final DefaultSessionCache cache = (DefaultSessionCache) sut.createSessionCache("a");
        // then: 'we use the correct hierarchical caches'
        assertEquals(globalVertexCache, ((DefaultHierarchicalCache) cache.getVertexCache()).parent);
        assertEquals(sessionVertexCache, ((DefaultHierarchicalCache) cache.getVertexCache()).child);
        assertEquals(globalEdgeCache, ((DefaultHierarchicalCache) cache.getEdgeCache()).parent);
        assertEquals(sessionEdgeCache, ((DefaultHierarchicalCache) cache.getEdgeCache()).child);

        // when: 'closing the cache'
        cache.close();
        // then: 'the caches are cleared and removed '
        verify(cacheManager, times(1)).removeCache("edge-session-a");
        verify(cacheManager, times(1)).removeCache("vertex-session-a");
    }

    @Test
    void close() throws Exception {
        // when: 'closing the cache manager'
        sut.close();
        // then: 'the cache manager has been closed as well'
        verify(cacheManager, times(1)).close();
        verify(cacheManager, times(1)).removeCache("edge-global");
        verify(cacheManager, times(1)).removeCache("vertex-global");

    }
}