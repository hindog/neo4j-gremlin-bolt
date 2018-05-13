package ta.nemahuta.neo4j.session;

/**
 * Rollback and commit operations interface.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public interface RollbackAndCommit {

    /**
     * Commits the scope to the database.
     */
    void commit();

    /**
     * Rolls back the scope.
     */
    void rollback();


}
