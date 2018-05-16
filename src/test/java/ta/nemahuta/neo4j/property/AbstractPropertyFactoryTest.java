package ta.nemahuta.neo4j.property;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ta.nemahuta.neo4j.query.TestNeo4JProperty;
import ta.nemahuta.neo4j.query.TestNeo4JPropertyFactory;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.testutils.MockUtils;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class AbstractPropertyFactoryTest {

    private TestNeo4JPropertyFactory sut = new TestNeo4JPropertyFactory();

    @ParameterizedTest
    @MethodSource("createFromValueSources")
    void createFromValue(final Pair<Object, VertexProperty.Cardinality> source) {
        assertCreate(source.getValue1(), source.getValue0());
    }

    static Stream<Pair<Object, VertexProperty.Cardinality>> createFromValueSources() {
        return Stream.of(
                new Pair<>("a", VertexProperty.Cardinality.single),
                new Pair<>(1, VertexProperty.Cardinality.single),
                new Pair<>(1d, VertexProperty.Cardinality.single),
                new Pair<>(1f, VertexProperty.Cardinality.single),
                new Pair<>(1l, VertexProperty.Cardinality.single),
                new Pair<>(true, VertexProperty.Cardinality.single),
                new Pair<>(new byte[1], VertexProperty.Cardinality.single),
                new Pair<>(ImmutableSet.of(), VertexProperty.Cardinality.list),
                new Pair<>(ImmutableList.of(), VertexProperty.Cardinality.list),
                new Pair<>(new Object(), VertexProperty.Cardinality.single),
                new Pair<>(null, null)
        );
    }

    void assertCreate(final VertexProperty.Cardinality cardinality, final Object source) {
        final Neo4JElement element = mock(Neo4JElement.class);
        final TestNeo4JProperty<?> result = sut.create(element, "x", MockUtils.mockValue(source));
        if (cardinality == null) {
            assertNull(result);
        } else {
            assertEquals("x", result.key());
            assertEquals(element, result.element());
            assertEquals(cardinality, result.getCardinality());
            if (source instanceof ImmutableSet) {
                assertEquals(source, ImmutableSet.copyOf((ImmutableList) result.value()));
            } else {
                assertEquals(source, result.value());
            }
        }
    }

}