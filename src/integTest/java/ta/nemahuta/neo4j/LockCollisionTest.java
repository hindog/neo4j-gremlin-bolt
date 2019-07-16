package ta.nemahuta.neo4j;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.COMPLEX;

class LockCollisionTest extends AbstractExampleGraphTest {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    void modifyGraphInParallel() throws Exception {
        streamGraph(COMPLEX.getSource());
        runInParallelAndAssert(this::createReturnGraphSize, true, 200);
    }

    @Nonnull
    private Callable<Boolean> createReturnGraphSize() {
        return () -> withGraphResult(graph -> {
            latch.countDown();
            final Vertex vertex1 = StreamSupport.stream(Spliterators.spliteratorUnknownSize(graph.vertices(), Spliterator.ORDERED), false)
                    .filter(propertyEquals("name", "HEY BO DIDDLEY")).findFirst().get();
            final Vertex vertex2 = StreamSupport.stream(Spliterators.spliteratorUnknownSize(graph.vertices(), Spliterator.ORDERED), false)
                    .filter(propertyEquals("name", "IM A MAN")).findFirst().get();
            final Iterator<Edge> inEdges = vertex1.edges(Direction.IN, "preceded_by");
            if (inEdges.hasNext()) {
                inEdges.next().remove();
            } else {
                vertex2.addEdge("preceded_by", vertex1);
            }
            vertex1.property("Change", UUID.randomUUID().toString());
            vertex2.property("Change", UUID.randomUUID().toString());
            return true;
        });
    }

}
