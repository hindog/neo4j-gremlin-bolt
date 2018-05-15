package ta.nemahuta.neo4j.query;

import org.neo4j.driver.v1.Statement;
import ta.nemahuta.neo4j.state.PropertyCardinality;
import ta.nemahuta.neo4j.state.PropertyValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractStatementBuilderTest {

    private static final AtomicLong propId = new AtomicLong(0);

    @Nonnull
    public static Statement build(@Nonnull final StatementBuilderAppender appender) {
        final StringBuilder sb = new StringBuilder();
        final Map<String, Object> parameters = new HashMap<>();
        appender.append(sb, parameters);
        return new Statement(sb.toString(), parameters);
    }

    public static void assertBuildsStatement(@Nonnull final String text, @Nonnull final Map<String, Object> parameters,
                                             @Nonnull final StatementBuilderAppender appender) {
        assertBuildsStatement(text, parameters, () -> Optional.of(build(appender)));
    }

    public static void assertBuildsStatement(@Nullable final String text, @Nullable final Map<String, Object> parameters,
                                             @Nonnull final StatementBuilder builder) {
        final Optional<Statement> stmtOpt = builder.build();
        if (text != null) {
            assertTrue(stmtOpt.isPresent());
            stmtOpt.ifPresent(stmt -> {
                assertEquals(text, stmt.text());
                assertEquals(Optional.ofNullable(parameters).orElseGet(Collections::emptyMap), stmt.parameters().asMap());
            });
        } else {
            assertFalse(stmtOpt.isPresent());
        }
    }

    public static <V> PropertyValue<V> prop(final V value) {
        final PropertyCardinality cardinality = (value instanceof Set) ? PropertyCardinality.SET :
                value instanceof Iterable ? PropertyCardinality.LIST : PropertyCardinality.SINGLE;
        return PropertyValue.from(propId.incrementAndGet(), value, cardinality);
    }

}
