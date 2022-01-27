package ta.nemahuta.neo4j.query;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * An interface for statement builder parts which contribute to the statement.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface StatementBuilderAppender {

    /**
     * Append the representation of this object to the query and parameters.
     *
     * @param queryBuilder the query builder to append to
     * @param parameters   the parameters to append to
     */
    void append(@Nonnull StringBuilder queryBuilder, @Nonnull Map<String, Object> parameters);
}
