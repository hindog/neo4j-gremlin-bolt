package ta.nemahuta.neo4j.async;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AsyncAccessTest {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AsyncAccess<String> sut = new AsyncAccess<>("");

    @Test
    void updateAndGet() {
        // setup: 'a new initial value'
        sut.update(s -> "synth");
        // when: 'updating the wrapped value'
        sut.update(s -> s + " wave");
        // then: 'the new value should be correct'
        assertEquals(sut.get(s -> s), "synth wave");
    }

    @Test
    void updateAndGetAsnyc() throws InterruptedException {
        // setup: 'an empty initial value and an executor service'
        final int limit = 2000;
        final Callable<String> updateCallable = () -> sut.update(s -> {
            latch.countDown();
            return s + ".";
        });

        sut.update(s -> "");
        final ExecutorService executorService = Executors.newCachedThreadPool();

        // when: 'scheduling some executions'
        executorService.invokeAll(Stream.generate(() -> updateCallable).limit(limit).collect(Collectors.toList()));

        // and: 'waiting for their execution'
        executorService.shutdown();

        // then: 'the length of the string is precisely the amount of calls we have made'
        final int actual = sut.get(String::length);
        assertEquals(actual, limit);
    }

    @Test
    void getThrows() {
        assertThrows(IllegalStateException.class, () -> sut.getThrows(s -> {
            throw new IllegalStateException("Nyan cat thrown");
        }));

    }

    @Test
    void consume() {
        boolean[] called = new boolean[1];
        sut.consume(s -> called[0] = true);
        assertEquals(called[0], true);
    }

}