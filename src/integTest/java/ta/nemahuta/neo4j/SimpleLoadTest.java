package ta.nemahuta.neo4j;

import com.google.common.collect.ImmutableSet;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.COMPLEX;

class SimpleLoadTest extends AbstractExampleGraphTest {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void checkMultipleReads() throws Exception {
        streamGraph(COMPLEX.getSource());
        runInParallelAndAssert(this::createReturnGraphSize, new Pair<>(COMPLEX.getVerticesCount(), COMPLEX.getEdgesCount()), 200);
    }

    @Nonnull
    private Callable<Pair<Integer, Integer>> createReturnGraphSize() {
        return () -> withGraphResult(graph -> {
            latch.countDown();
            return new Pair<>(ImmutableSet.copyOf(graph.vertices()).size(), ImmutableSet.copyOf(graph.edges()).size());
        });
    }

}
