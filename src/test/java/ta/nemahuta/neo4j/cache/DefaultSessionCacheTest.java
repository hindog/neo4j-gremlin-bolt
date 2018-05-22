package ta.nemahuta.neo4j.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class DefaultSessionCacheTest {

    @Mock
    private HierarchicalCache<Long, Neo4JEdgeState> edgeCache;
    @Mock
    private HierarchicalCache<Long, Neo4JVertexState> vertexCache;

    @Test
    void returnsCaches() {
        // when: 'creating the cache'
        final DefaultSessionCache sut = new DefaultSessionCache(edgeCache, vertexCache) {
            @Override
            public void close() {
            }
        };
        // then: 'the correct child caches are returned'
        assertEquals(edgeCache, sut.getEdgeCache());
        assertEquals(vertexCache, sut.getVertexCache());
    }
}