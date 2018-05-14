package ta.nemahuta.neo4j.query;

import org.neo4j.driver.v1.Statement;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    public static void assertBuildsStatement(@Nonnull final String text, @Nonnull final Map<String, Object> parameters,
                                             @Nonnull final StatementBuilder builder) {
        final Optional<Statement> stmtOpt = builder.build();
        assertTrue(stmtOpt.isPresent());
        stmtOpt.ifPresent(stmt -> {
            assertEquals(text, stmt.text());
            assertEquals(parameters, stmt.parameters().asMap());
        });
    }


}
