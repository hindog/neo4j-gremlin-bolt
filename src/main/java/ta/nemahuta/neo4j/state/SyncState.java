package ta.nemahuta.neo4j.state;

import javax.annotation.Nonnull;

/**
 * Enumeration denoting the synchronization state.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public enum SyncState {

    /**
     * The object is transient, thus only existent in the session.
     */
    TRANSIENT("Added to the session, not persisted in the graph"),
    /**
     * The object is modified, thus existent on the remote side but changed in the session.
     */
    MODIFIED("Loaded from the graph, but the state was changed"),
    /**
     * The object is deleted, thus marked for deletion in the session and existent on the remote side.
     */
    DELETED("Loaded from the graph, but marked as deleted"),
    /**
     * The object is synchronous, thus the session holds the same state as loaded from the remote side.
     */
    SYNCHRONOUS("Loaded from the graph, the state is synchronous"),
    /**
     * The object was discarded, thus it can be removed from the session.
     */
    DISCARDED("Not loaded from the graph, can be discarded");

    private final String description;

    SyncState(final String description) {
        this.description = description;
    }

    /**
     * @return the state converted to a modified state
     */
    @Nonnull
    public SyncState asModified() {
        switch (this) {
            case TRANSIENT:
                return TRANSIENT;
            case DISCARDED:
                return DISCARDED;
            case MODIFIED:
            case SYNCHRONOUS:
                return MODIFIED;
            case DELETED:
                return DELETED;
            default:
                throw new IllegalStateException("Cannot map state " + this + " to a modified state");
        }
    }

    /**
     * @return the state converted to a deleted state
     */
    @Nonnull
    public SyncState asDeleted() {
        switch (this) {
            case TRANSIENT:
            case DISCARDED:
                return DISCARDED;
            case MODIFIED:
            case SYNCHRONOUS:
            case DELETED:
                return DELETED;
            default:
                throw new IllegalStateException("Cannot map state " + this + " to a deleted state");
        }
    }

    @Nonnull
    public SyncState asSynchronized() {
        switch (this) {
            case DELETED:
            case DISCARDED:
                return DISCARDED;
            case TRANSIENT:
            case SYNCHRONOUS:
            case MODIFIED:
                return SYNCHRONOUS;
            default:
                throw new IllegalStateException("Cannot map state " + this + " to a synchronized state");
        }
    }
}
