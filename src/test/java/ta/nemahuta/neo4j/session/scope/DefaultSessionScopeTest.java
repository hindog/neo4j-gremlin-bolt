package ta.nemahuta.neo4j.session.scope;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.SessionScope;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultSessionScopeTest {

    @Mock
    private Neo4JElementScope<Neo4JVertex> vertexScope;
    @Mock
    private Neo4JEdgeScope edgeScope;

    private SessionScope sut;

    @BeforeEach
    void pfeffi() {
        sut = new DefaultSessionScope(vertexScope, edgeScope);
    }

    @Test
    void commitInOrder() {
        final InOrder order = inOrder(vertexScope, edgeScope);
        // when: 'committing'
        sut.commit();
        // then: 'the commits are done in the correct order'
        order.verify(vertexScope).commit();
        order.verify(edgeScope).commit();
        verifyNoMoreInteractions(vertexScope, edgeScope);
    }

    @Test
    void rollbackInOrder() {
        final InOrder order = inOrder(edgeScope, vertexScope);
        // when: 'committing'
        sut.rollback();
        // then: 'the commits are done in the correct order'
        order.verify(edgeScope).rollback();
        order.verify(vertexScope).rollback();
        verifyNoMoreInteractions(vertexScope, edgeScope);
    }

    @Test
    void flush() {
        // when: 'flushing the scope'
        sut.flush();
        // then: 'all scopes have been flushed'
        verify(vertexScope).flush();
        verify(edgeScope).flush();
        verifyNoMoreInteractions(vertexScope, edgeScope);
    }

}