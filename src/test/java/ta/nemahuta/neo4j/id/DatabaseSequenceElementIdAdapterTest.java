package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Entity;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class DatabaseSequenceElementIdAdapterTest {

    @Mock
    private Driver driver;
    @Mock
    private Session session;
    @Mock
    private Entity entity;
    @Mock
    private Transaction transaction;

    private Neo4JElementIdAdapter<?> sut;

    @BeforeEach
    void setupAdapter() {
        this.sut = new DatabaseSequenceElementIdAdapter(driver);
    }

    private void stubDriverAndSession() {
        when(driver.session()).thenReturn(session);
        when(session.beginTransaction()).thenReturn(transaction);
    }

    @Test
    void propertyName() {
        assertEquals(sut.propertyName(), "id");
    }

    @Test
    void retrieveId() {
        // setup: 'the entity returning an id'
        final long id = 666l;
        final Value value = mock(Value.class);
        when(value.asLong()).thenReturn(id);
        when(entity.get(sut.propertyName())).thenReturn(value);
        // when: 'retrieving the identifier from the entity'
        final Neo4JElementId<?> actual = sut.retrieveId(entity);
        // then: 'the id is wrapped into a persistent element id'
        assertEquals(actual, new Neo4JPersistentElementId<>(id));
    }

    @Test
    void generateDoesComplainIfNoRecord() {
        // setup: 'a mocked next batch which is lower than the current one'
        stubDriverAndSession();
        mockNextId(null);
        // when: 'requesting a new id'
        Executable fun = () -> sut.generate();
        // then: 'an exception is thrown'
        assertTimeoutPreemptively(Duration.ofSeconds(1l), () -> assertThrows(IllegalStateException.class, fun));
    }

    @Test
    void generateDoesNotAcceptDescending() {
        // setup: 'a mocked next batch which is lower than the current one'
        stubDriverAndSession();
        mockNextId(i -> -1l);
        // when: 'requesting a new id'
        Executable fun = () -> sut.generate();
        // then: 'an exception is thrown'
        assertTimeoutPreemptively(Duration.ofSeconds(1l), () -> assertThrows(IllegalStateException.class, fun));
    }

    @Test
    void generate() {
        // setup: 'a mocked next batch which is lower than the current one'
        stubDriverAndSession();
        final Iterator<Long> iter = Stream.of(DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE, DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE, DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE * 2).iterator();
        mockNextId(i -> iter.next());

        assertTimeoutPreemptively(Duration.ofSeconds(5l), () -> {
            // when: 'requesting a new id'
            final Neo4JElementId<?> newId = sut.generate();
            // then: 'the next id is generated'
            assertEquals(new Neo4JPersistentElementId<>(1l), newId);
            // when: 'requesting another pool by requesting the ids'
            Stream.generate(() -> sut.generate()).limit(DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE*2-1).forEach(r -> assertTrue(r.isRemote()));
            // then: 'the pool request should have been invoked three times (one for the first pool, the second for the invalid, the third for the new one'
            verify(transaction, times(3)).run(any(Statement.class));
        });


    }

    @Test
    void generateParallel() throws InterruptedException {
        // setup: 'a test executor and an identifier generator'
        final long limit = DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE * 20l;
        final AtomicLong generator = new AtomicLong(0l);
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(1);
        stubDriverAndSession();
        mockNextId(i -> generator.addAndGet(DatabaseSequenceElementIdAdapter.DEFAULT_POOL_SIZE));
        final Callable<Neo4JElementId<?>> callable = () -> {
            latch.countDown();
            return sut.generate();
        };
        // when: 'scheduling the generation of the items'
        final List<Future<Neo4JElementId<?>>> futures = executorService.invokeAll(Stream.generate(() -> callable).limit(limit).collect(Collectors.toList()));
        // and: 'waiting for the tasks to be completed and collect the ids'
        executorService.shutdown();
        final List<Neo4JElementId<?>> ids = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (final InterruptedException | ExecutionException e) {
                throw new IllegalStateException("Could not wait for future", e);
            }
        }).collect(Collectors.toList());
        // then: 'all ids are transients'
        assertTrue(ids.stream().allMatch(Neo4JElementId::isRemote));
        // and: 'the ids are distinct'
        assertEquals(ids.stream().map(Neo4JElementId::getId).collect(Collectors.toSet()).size(), limit);
    }


    private void mockNextId(final Answer<Long> idAnswer) {
        final StatementResult result = mock(StatementResult.class);
        when(result.hasNext()).thenReturn(idAnswer != null);
        if (idAnswer != null) {
            final Record record = mock(Record.class);
            when(result.next()).thenReturn(record);
            final Value value = mock(Value.class);
            when(record.get(0)).thenReturn(value);
            when(value.asLong()).thenAnswer(idAnswer);
        }
        when(transaction.run(any(Statement.class))).thenReturn(result);
    }

}
