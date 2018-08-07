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
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
    private Statement deleteStmt, createStmt, updateStmt, loadStmt, createIndexStmt, queryStmt;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private StatementExecutor statementExecutor;

    @Mock
    private Record record;

    @Mock
    private TypeRepresentation type;

    @Mock
    private Value value;

    @Mock
    private AbstractQueryBuilder queryBuilder;

    private AbstractNeo4JElementStateHandler<Neo4JElementState, AbstractQueryBuilder> sut;

    @BeforeEach
    void stubStatements() {
        sut = new AbstractNeo4JElementStateHandler<Neo4JElementState, AbstractQueryBuilder>(statementExecutor) {
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

            @Nonnull
            @Override
            protected Statement createCreateIndexCommand(@Nonnull final Set<String> labels, final String propertyName) {
                return createIndexStmt;
            }

            @Nonnull
            @Override
            protected AbstractQueryBuilder query() {
                return queryBuilder;
            }
        };
        when(statementExecutor.executeStatement(any())).then(i -> {
            final Iterator<Record> iter = Stream.of(record).iterator();
            final StatementResult result = mock(StatementResult.class);
            when(result.hasNext()).then(ii -> iter.hasNext());
            when(result.next()).then(ii -> iter.next());
            return result;
        });
        when(queryBuilder.build()).thenReturn(Optional.of(queryStmt));
        when(record.get(0)).thenReturn(value);
        when(type.constructor()).thenReturn(TypeConstructor.NUMBER);
        when(value.type()).thenReturn(type);
        when(value.asNumber()).thenReturn(2l);
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

    @Test
    void createIndex() {
        // when: ''
        sut.createIndex(ImmutableSet.of("x"), "property");
        verify(statementExecutor, times(1)).executeStatement(createIndexStmt);
    }

    @Test
    void query() {
        final Function<AbstractQueryBuilder, AbstractQueryBuilder> queryFun = mock(Function.class);
        when(queryFun.apply(any())).then(i -> i.getArgument(0));

        // when: 'applying the query'
        assertEquals(ImmutableMap.of(1l, state), ImmutableMap.copyOf(sut.query(queryFun)));

        // and: 'the query statements are executed'
        verify(statementExecutor, times(1)).executeStatement(queryStmt);
        verify(statementExecutor, times(1)).retrieveRecords(queryStmt);
    }

}