package ta.nemahuta.neo4j.session;

import lombok.NonNull;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Interface for an executor which executes a {@link Statement} and returns a {@link StatementResult}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface StatementExecutor {

    /**
     * Execute a statement and return a result.
     *
     * @param statement the statement to be executed
     * @return the result of the execution
     */
    @Nullable
    StatementResult executeStatement(@Nonnull Statement statement);

    /**
     * @return a {@link Stream} of the {@link Record}s in the {@link StatementResult}
     * @see #executeStatement(Statement)
     */
    @Nonnull
    default Stream<Record> retrieveRecords(@Nonnull @NonNull final Statement statement) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(executeStatement(statement),
                        Spliterator.NONNULL | Spliterator.IMMUTABLE
                ), false);
    }

}
