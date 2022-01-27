package ta.nemahuta.neo4j.session;

import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Interface for an executor which executes a {@link Statement} and returns a {@link Result}.
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
    Result executeStatement(@Nonnull Query statement);

    /**
     * Retrieve the records of a statement execution through the {@link Result}.
     *
     * @param statement the statement to retrieve the records for
     * @return a {@link Stream} of the {@link Record}s in the {@link Result}
     */
    @Nonnull
    default Stream<Record> retrieveRecords(@Nonnull final Query statement) {
        final Result iterator = executeStatement(statement);
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.NONNULL | Spliterator.IMMUTABLE
                ), true);
    }

}
