package ta.nemahuta.neo4j;

import org.junit.jupiter.api.Test;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static ta.nemahuta.neo4j.AbstractExampleGraphTest.ExampleGraphs.COMPLEX;

public class CacheTest extends AbstractExampleGraphTest {

    @Test
    void elementRemoval() throws Exception {
        streamGraph(COMPLEX.getSource());
        withGraph(graph ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(graph.vertices(), Spliterator.ORDERED), false)
                        .filter(propertyEquals("name", "HEY BO DIDDLEY")).findFirst().get().remove()
        );
        final boolean exists = withGraphResult(graph ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(graph.vertices(), Spliterator.ORDERED), false)
                        .filter(propertyEquals("name", "HEY BO DIDDLEY")).findFirst().isPresent()
        );
        assertFalse(exists);
    }


}
