package ta.nemahuta.neo4j.scope;

import ta.nemahuta.neo4j.session.RollbackAndCommit;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

public interface Neo4JElementStateScope<S extends Neo4JElementState> extends RollbackAndCommit {

    /**
     * Modifies the element state for the element with the provided id.
     * using a new state.
     *
     * @param id       the identifier of the element
     * @param newState the new state
     */
    void update(long id,
                @Nonnull S newState);

    /**
     * Deletes the state for the element with the provided ids.
     *
     * @param id the identifier for the element to be deleted
     */
    void delete(long id);

    /**
     * Create a new entry for the provided state returning the identifier
     *
     * @param state the state which to create
     * @return the identifier
     */
    long create(@Nonnull S state);

    /**
     * Get the state of a single element.
     *
     * @param id the id of the element
     * @return the state of the element or {@code null} if none is known by this id
     */
    @Nullable
    S get(long id);

    /**
     * Get the element with the provided ids.
     *
     * @param ids the identifiers to getAll the elements for
     * @return the elements which have been found for the ids
     */
    @Nonnull
    Map<Long, S> getAll(@Nonnull Collection<Long> ids);

}
