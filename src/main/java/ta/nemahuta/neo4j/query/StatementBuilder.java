package ta.nemahuta.neo4j.query;

import org.neo4j.driver.v1.Statement;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Interface for a statement builder.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface StatementBuilder {

    /**
     * @return build the {@link Statement}, if a statement is required
     */
    @Nonnull
    Optional<Statement> build();

}
