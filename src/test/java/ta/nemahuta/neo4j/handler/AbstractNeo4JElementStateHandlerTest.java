package ta.nemahuta.neo4j.handler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AbstractNeo4JElementStateHandlerTest {

    @Mock
    private Neo4JElementState state;

    @Mock
    private Statement deleteStmt, createStmt, updateStmt, loadStmt;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private StatementExecutor statementExecutor;

    @Mock
    private Record record;

    @Mock
    private Value value;

    private AbstractNeo4JElementStateHandler<Neo4JElementState> sut;

    @BeforeEach
    void stubStatements() {
        sut = new AbstractNeo4JElementStateHandler<Neo4JElementState>(statementExecutor) {
            @Override
            protected Pair<Long, Neo4JElementState> getIdAndConvertToState(final Record r) {
                return new Pair<>(1l, state);
            }

            @Nonnull
            @Override
            protected Statement createDeleteCommand(final long id) {
                return deleteStmt;
            }

            @Nonnull
            @Override
            protected Statement createUpdateCommand(final long id, final Neo4JElementState currentState, final Neo4JElementState state) {
                return updateStmt;
            }

            @Nonnull
            @Override
            protected Statement createInsertCommand(@Nonnull final Neo4JElementState state) {
                return createStmt;
            }

            @Nonnull
            @Override
            protected Statement createLoadCommand(@Nonnull final Set<Long> ids) {
                return loadStmt;
            }
        };
        when(statementExecutor.executeStatement(any())).then(i -> {
            final Iterator<Record> iter = Stream.of(record).iterator();
            final StatementResult result = mock(StatementResult.class);
            when(result.hasNext()).then(ii -> iter.hasNext());
            when(result.next()).then(ii -> iter.next());
            return result;
        });
        when(record.get(0)).thenReturn(value);
        when(value.asLong()).thenReturn(2l);
        when(record.size()).thenReturn(1);
    }

    @AfterEach
    void checkNoMoreInteractions() {
        verifyNoMoreInteractions(statementExecutor);
    }

    @Test
    void getAll() {
        // when: 'requesting elements'
        assertEquals(ImmutableMap.of(1l, state), sut.getAll(ImmutableSet.of(1l, 2l)));
        // then: 'the load command was invoked'
        verify(statementExecutor, times(1)).executeStatement(loadStmt);
        verify(statementExecutor, times(1)).retrieveRecords(loadStmt);
    }

    @Test
    void update() {
        // when: 'updating an element'
        sut.update(2l, state, state);
        // then: 'the load command was invoked'
        verify(statementExecutor, times(1)).executeStatement(updateStmt);
    }

    @Test
    void delete() {
        // when: 'deleting an element'
        sut.delete(1l);
        // then: 'the load command was invoked'
        verify(statementExecutor, times(1)).executeStatement(deleteStmt);
    }

    @Test
    void create() {
        // when: 'creating an element'
        assertEquals(2l, sut.create(state));
        // then: 'the insert command was invoked'
        verify(statementExecutor, times(1)).executeStatement(createStmt);
        verify(statementExecutor, times(1)).retrieveRecords(createStmt);
    }
}