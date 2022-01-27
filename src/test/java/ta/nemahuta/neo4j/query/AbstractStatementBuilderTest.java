package ta.nemahuta.neo4j.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.executables.ExecutableStatement;
import org.neo4j.cypherdsl.parser.CypherParser;
import org.neo4j.driver.Query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractStatementBuilderTest {

    @Nonnull
    public static Query build(@Nonnull final StatementBuilderAppender appender) {
        final StringBuilder sb = new StringBuilder();
        final Map<String, Object> parameters = new HashMap<>();
        appender.append(sb, parameters);
        return new Query(sb.toString().trim(), parameters);
    }

    public static void assertBuildsStatement(@Nonnull final String text, @Nonnull final Map<String, Object> parameters,
                                             @Nonnull final StatementBuilderAppender appender) {
        assertBuildsStatement(text, parameters, () -> Optional.of(build(appender)));
    }

    public static void assertBuildsStatement(@Nullable final String text, @Nullable final Map<String, Object> parameters,
                                             @Nonnull final StatementBuilder builder) {
        final Optional<Query> stmtOpt = builder.build();
        if (text != null) {
            assertTrue(stmtOpt.isPresent());
            stmtOpt.ifPresent(stmt -> assertStatement(text, parameters, stmt));
        } else {
            assertFalse(stmtOpt.isPresent());
        }
    }

    public static void assertStatement(@Nonnull final String text, final @Nullable Map<String, Object> parameters, final Query stmt) {
        assertEquals(text, stmt.text());
        assertEquals(transformParameters(parameters), transformParameters(stmt.parameters().asMap()));
    }

    @Nonnull
    private static Map<String, Object> transformParameters(@Nullable final Map<String, Object> source) {
        if (source == null) {
            return ImmutableMap.of();
        }
        return ImmutableMap.copyOf(Maps.transformValues(source, e -> {
            if (e instanceof Iterable) {
                return ImmutableSet.copyOf((Iterable) e);
            } else if (e instanceof Map) {
                return ImmutableMap.copyOf((Map) e);
            } else {
                return e;
            }
        }));
    }

}
