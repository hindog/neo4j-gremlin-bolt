package ta.nemahuta.neo4j.query;

/**
 * Marker interface for an operation.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface Operation extends StatementBuilderAppender {

    /**
     * @return {@code true} if a statement for the operation is need, {@code false} otherwise
     */
    boolean isNeedsStatement();

}
