package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.v1.types.Entity;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Neo4JNativeElementIdAdapterTest {

    @Mock
    private Entity entity;

    private final Neo4JElementIdAdapter<?> sut = new Neo4JNativeElementIdAdapter();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void propertyName() {
        assertEquals(sut.propertyName(), "id");
    }

    @Test
    void retrieveId() {
        // setup: 'the entity returning an id'
        final long id = 666l;
        when(entity.id()).thenReturn(id);
        // when: 'retrieving the identifier from the entity'
        final Neo4JElementId<?> actual = sut.retrieveId(entity);
        // then: 'the id is wrapped into a persistent element id'
        assertEquals(actual, new Neo4JPersistentElementId<>(id));
    }

    @Test
    void generate() throws InterruptedException {
        // setup: 'an executor service'
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final int limit = 2000;
        final Callable<Neo4JElementId<?>> callable = sut::generate;
        // when: 'scheduling the generation of elements'
        final List<Future<Neo4JElementId<?>>> futures = executorService.invokeAll(Stream.generate(() -> callable).limit(limit).collect(Collectors.toList()));
        // and: 'waiting for the generation to finish'
        executorService.shutdown();
        final List<Neo4JElementId<?>> ids = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (final InterruptedException | ExecutionException e) {
                throw new IllegalStateException("Could not wait for future", e);
            }
        }).collect(Collectors.toList());
        // then: 'all ids are transients'
        assertFalse(ids.stream().anyMatch(Neo4JElementId::isRemote));
        // and: 'the ids are distinct'
        assertEquals(ids.stream().map(Neo4JElementId::getId).collect(Collectors.toSet()).size(), limit);
    }
}