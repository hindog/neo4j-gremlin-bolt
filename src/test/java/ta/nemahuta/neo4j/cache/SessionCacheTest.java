package ta.nemahuta.neo4j.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SessionCacheTest {

    @Mock
    private HierarchicalCache<Long, Neo4JEdgeState> edgeCache;
    @Mock
    private HierarchicalCache<Long, Neo4JVertexState> vertexCache;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private SessionCache sessionCache;

    @BeforeEach
    void stubCalls() {
        when(sessionCache.getEdgeCache()).thenReturn(edgeCache);
        when(sessionCache.getVertexCache()).thenReturn(vertexCache);
    }

    @Test
    void flush() {
        // when: 'calling flush on the session cache'
        sessionCache.flush();
        // then: 'all caches are cleared'
        verify(edgeCache, times(1)).clear();
        verify(vertexCache, times(1)).clear();
        verifyNoMoreInteractions(edgeCache, vertexCache);
    }

    @Test
    void commit() {
        // when: 'calling flush on the session cache'
        sessionCache.commit();
        // then: 'all caches are cleared'
        verify(edgeCache, times(1)).commit();
        verify(vertexCache, times(1)).commit();
        verifyNoMoreInteractions(edgeCache, vertexCache);
    }

}