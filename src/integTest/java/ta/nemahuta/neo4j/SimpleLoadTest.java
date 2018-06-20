package ta.nemahuta.neo4j;

import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.COMPLEX;

class SimpleLoadTest extends AbstractExampleGraphTest {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void checkMultipleReads() throws Exception {
        streamGraph(COMPLEX.getSource());
        final int amount = 200;

        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<Callable<Pair<Integer, Integer>>> callables = Stream.generate(this::createReturnGraphSize).limit(amount).collect(Collectors.toList());
        final List<Future<Pair<Integer, Integer>>> futures = executor.invokeAll(callables);
        executor.shutdown();
        final List<Pair<Integer, Integer>> results = new ArrayList<>();
        for (final Future<Pair<Integer, Integer>> future : futures) {
            results.add(future.get());
        }
        final List<Pair<Integer, Integer>> expected = Stream.generate(() -> new Pair<>(COMPLEX.getVerticesCount(), COMPLEX.getEdgesCount()))
                .limit(amount).collect(Collectors.toList());
        results.removeAll(expected);
        assertEquals(Collections.emptyList(), results);
    }

    @Nonnull
    private Callable<Pair<Integer, Integer>> createReturnGraphSize() {
        return () -> withGraph(graph -> {
            latch.countDown();
            return new Pair<>(ImmutableSet.copyOf(graph.vertices()).size(), ImmutableSet.copyOf(graph.edges()).size());
        });
    }

}
