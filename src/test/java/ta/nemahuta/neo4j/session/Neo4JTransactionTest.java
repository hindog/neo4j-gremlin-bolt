package ta.nemahuta.neo4j.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.MapValue;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class Neo4JTransactionTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Session session;

    @Mock
    private Transaction transaction;

    @Mock
    private Query statement;

    private Neo4JTransaction sut;

    @BeforeEach
    void createSut() {
        this.sut = new Neo4JTransaction(graph, session);
    }

    private void stubTransactionAndOpen() {
        when(session.beginTransaction()).thenReturn(transaction);
        this.sut.open();
    }


    @Test
    void doOpen() {
        // when: 'stubbing the transaction and opening it'
        stubTransactionAndOpen();
        // then: 'the transaction is open'
        assertTrue(this.sut.isOpen());
        // and: 'a new transaction was requested'
        verify(session, times(1)).beginTransaction();
    }

    @Test
    void doCommit() {
        // setup: 'stub the transaction'
        stubTransactionAndOpen();
        // when: 'committing the transaction'
        sut.doCommit();
        // then: 'the transaction was marked as success and the cache was committed'
        verify(transaction, times(1)).commit();
    }


    @Test
    void doRollback() {
        // setup: 'stub the transaction'
        stubTransactionAndOpen();
        // when: 'committing the transaction'
        sut.doRollback();
        // then: 'the transaction was marked as failure and the cache was flushed'
        verify(transaction, times(1)).rollback();
    }

    @Test
    void close() {
        // setup: 'stub the transaction'
        stubTransactionAndOpen();
        // when: 'closing the transaction'
        sut.close();
        // then: 'the transaction was closed, the session cache was closed and it is not open anymore'
        assertFalse(sut.isOpen());
        verify(transaction, times(1)).close();
    }

    @Test
    void executeStatement() {
        // setup: 'stub the transaction'
        Value parameters = new MapValue(Collections.emptyMap());
        when(session.beginTransaction()).thenReturn(transaction);
        when(statement.text()).thenReturn("foo");
        when(statement.parameters()).thenReturn(parameters);

        // when: 'closing the transaction'
        sut.executeStatement(statement);
        // then: 'the execution is delegated'
        verify(transaction, times(1)).run("foo", parameters.asMap());
        // and: 'a new transaction was requested'
        verify(session, times(1)).beginTransaction();
    }

}