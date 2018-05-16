package ta.nemahuta.neo4j.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractStatementBuilderTest {

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

}
