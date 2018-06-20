package ta.nemahuta.neo4j.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.AuthTokens;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JCacheSessionCacheManagerTest {

    @Mock
    private CacheManager cacheManager;

    @Mock
    private Cache<Long, Neo4JVertexState> globalVertexCache;
    @Mock
    private Cache<Long, Neo4JEdgeState> globalEdgeCache;

    private SessionCacheManager sut;

    @BeforeEach
    void stubGlobalCreationAndCreateSut() {
        when(cacheManager.createCache(startsWith("vertex-global-"), any(Configuration.class))).thenReturn(globalVertexCache);
        when(cacheManager.createCache(startsWith("edge-global-"), any(Configuration.class))).thenReturn(globalEdgeCache);
        final Neo4JConfiguration config = Neo4JConfiguration.builder().hostname("localhost").port(1234).authToken(AuthTokens.none()).build();
        this.sut = new JCacheSessionCacheManager(cacheManager, config);
    }


    @Test
    void createSessionCache() {
        // when: 'creating a session cache'
        final DefaultSessionCache cache = (DefaultSessionCache) sut.createSessionCache("a");
        // then: 'we use the correct hierarchical caches'
        assertEquals(globalVertexCache, ((HierarchicalJCache) cache.getVertexCache()).parent);
        assertNotNull(((HierarchicalJCache) cache.getVertexCache()).child);
        assertEquals(globalEdgeCache, ((HierarchicalJCache) cache.getEdgeCache()).parent);
        assertNotNull(((HierarchicalJCache) cache.getEdgeCache()).child);
    }

    @Test
    void close() throws Exception {
        // when: 'closing the cache manager'
        sut.close();
        // then: 'the cache manager has been closed as well'
        verify(cacheManager, times(1)).destroyCache(startsWith("edge-global-"));
        verify(cacheManager, times(1)).destroyCache(startsWith("vertex-global-"));

    }
}