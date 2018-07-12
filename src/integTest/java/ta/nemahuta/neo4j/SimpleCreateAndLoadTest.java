package ta.nemahuta.neo4j;

import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.COMPLEX;
import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.SIMPLE;

class SimpleCreateAndLoadTest extends AbstractExampleGraphTest {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void checkCreateAndReadSmallGraph() throws Exception {
        checkGraph(SIMPLE);
        withGraph(graph -> {
            final Optional<Vertex> joshOpt = ImmutableList.copyOf(graph.vertices()).stream()
                    .filter(v -> Objects.equals(v.property("name").value(), "josh")).findAny();
            assertTrue(joshOpt.isPresent(), "Josh is not present");
            final Vertex josh = joshOpt.get();
            assertTrue(josh.property("age").isPresent());
            josh.property("age").remove();
            assertFalse(josh.property("age").isPresent());
            assertTrue(josh.edges(Direction.OUT).hasNext(), "No out edges for josh");
            josh.remove();
            assertTrue(ImmutableList.copyOf(graph.vertices()).stream()
                    .noneMatch(v -> Objects.equals(v.property("name").value(), "josh")));
            graph.tx().commit();
        });
        withGraph(graph -> {
            assertTrue(ImmutableList.copyOf(graph.vertices()).stream()
                    .noneMatch(v -> Objects.equals(v.property("name").value(), "josh")));
        });
    }

    @Test
    void checkCreateAndReadHugeGraph() throws Exception {
        checkGraph(COMPLEX);
    }

    @Test
    void parallelTest1() throws Exception {
        parallelStream(1000, "/graph1-example.xml");
    }

    @Test
    void parallelTest2() throws Exception {
        parallelStream(10, "/graph2-example.xml");
    }

    @Test
    void edgesAreRegisteredOnBothEnds() throws Exception {
        final AtomicReference<Object> vertexId = new AtomicReference<Object>();
        withGraph(graph -> {
            assertFalse(graph.vertices().hasNext());
            final Vertex vertex1 = graph.addVertex("hallo");
            vertexId.set(vertex1.id());
            final Vertex vertex2 = graph.addVertex("hejsan");
            assertFalse(vertex1.edges(Direction.OUT, "svenska").hasNext());
            vertex1.addEdge("svenska", vertex2);
            assertEquals(vertex1, graph.vertices(vertex1.id()).next());
            assertTrue(vertex2.edges(Direction.IN, "svenska").hasNext());
            assertTrue(graph.vertices(vertex2.id()).next().edges(Direction.IN, "svenska").hasNext());
        });
    }

    private void parallelStream(final int max, final String source) throws InterruptedException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        final List<Future<Boolean>> futures = executor.invokeAll(
                Stream.<Callable<Boolean>>generate(() -> () -> {
                    latch.countDown();
                    try {
                        streamGraph(source);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    return true;
                }).limit(max).collect(Collectors.toList())
        );
        executor.shutdown();
        assertTrue(futures.stream().allMatch(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }));
    }

}
