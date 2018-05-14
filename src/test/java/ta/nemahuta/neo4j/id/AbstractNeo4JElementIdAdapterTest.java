package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class AbstractNeo4JElementIdAdapterTest {

    @Mock(answer = Answers.CALLS_REAL_METHODS)
    private AbstractNeo4JElementIdAdapter sut;

    @ParameterizedTest
    @MethodSource("convertSource")
    void convert(@Nonnull final TestData testData) {
        assertEquals(sut.convert(testData.source), testData.expected);
    }

    @ParameterizedTest
    @MethodSource("convertIllegalSource")
    void convertIllegalSource(@Nonnull final TestData testData) {
        final Executable fun = () -> sut.convert(testData.source);
        assertThrows(IllegalArgumentException.class, fun);
    }

    static Stream<TestData> convertSource() {
        return Stream.of(
                new TestData("1", new Neo4JPersistentElementId<>(1l)),
                new TestData(2l, new Neo4JPersistentElementId<>(2l)),
                new TestData((byte) 3, new Neo4JPersistentElementId<>(3l)),
                new TestData('4', new Neo4JPersistentElementId<>(4l)),
                new TestData(5, new Neo4JPersistentElementId<>(5l)),
                new TestData(new Neo4JPersistentElementId<>("6"), new Neo4JPersistentElementId<>(6l)),
                new TestData(new Neo4JPersistentElementId<>(7l), new Neo4JPersistentElementId<>(7l)),
                new TestData(new Neo4JPersistentElementId<>((byte) 8), new Neo4JPersistentElementId<>(8l)),
                new TestData(new Neo4JPersistentElementId<>('9'), new Neo4JPersistentElementId<>(9l)),
                new TestData(new Neo4JPersistentElementId<>(10), new Neo4JPersistentElementId<>(10l))
        );
    }

    static Stream<TestData> convertIllegalSource() {
        return Stream.of(
                new TestData(new Neo4JTransientElementId<>(1l), null),
                new TestData(null, null),
                new TestData("1.2", null),
                new TestData(1.2d, null),
                new TestData(1.2f, null)
        );
    }

    static class TestData {

        private final Object source;
        private final Neo4JElementId<?> expected;

        TestData(final Object source, final Neo4JElementId<?> expected) {
            this.source = source;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return source + Optional.ofNullable(source).map(s -> "(" + s.getClass().getSimpleName() + ")").orElse("") + " -> " +
                    Optional.ofNullable(expected).map(Object::toString).orElseGet(IllegalArgumentException.class::getSimpleName);
        }
    }

}