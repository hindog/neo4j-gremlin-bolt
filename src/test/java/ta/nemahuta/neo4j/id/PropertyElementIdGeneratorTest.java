package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyElementIdGeneratorTest {

    private final PropertyElementIdGenerator sut = new PropertyElementIdGenerator();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void generate() throws InterruptedException {
        // setup: 'a new executor service for parallel execution'
        final int limit = 2000;
        final Callable<Neo4JElementId<?>> callable = () -> {
            latch.countDown();
            return sut.generate();
        };
        final ExecutorService executorService = Executors.newCachedThreadPool();
        // when: 'scheduling the generation'
        final List<Future<Neo4JElementId<?>>> futures = executorService.invokeAll(
                Stream.generate(() -> callable).limit(limit).collect(Collectors.toList()));
        // and: 'waiting for their computation'
        executorService.shutdown();
        // then: 'the amount of generated distinct ids is exactly the limit'
        assertEquals(futures.stream().collect(Collectors.toSet()).size(), limit);
    }

    private Neo4JElementId<Long> generateId() {
        latch.countDown();
        return sut.generate();
    }
}