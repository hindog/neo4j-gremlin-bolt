package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

import java.util.Iterator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatementExecutorTest {

    @Mock
    private Result statementResult;

    @Mock
    private Query statement;

    @Mock
    private Record record1, record2;

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private StatementExecutor sut;

    @BeforeEach
    void stubResult() {
        when(sut.executeStatement(any())).thenReturn(statementResult);
    }

    @Test
    void retrieveRecordsEmpty() {
        // setup: 'empty result'
        when(statementResult.hasNext()).thenReturn(false);
        // when: 'retrieving the records'
        final Stream<Record> records = sut.retrieveRecords(statement);
        // then: 'no records are provided'
        assertEquals(ImmutableList.of(), ImmutableList.copyOf(records.iterator()));
    }

    @Test
    void retrieveRecords() {
        // setup: 'empty result'
        final Iterator<Record> iter = Stream.of(record1, record2).iterator();
        when(statementResult.hasNext()).then(i -> iter.hasNext());
        when(statementResult.next()).then(i -> iter.next());
        // when: 'retrieving the records'
        final Stream<Record> records = sut.retrieveRecords(statement);
        // then: 'no records are provided'
        assertEquals(ImmutableList.of(record1, record2), ImmutableList.copyOf(records.iterator()));
    }

}