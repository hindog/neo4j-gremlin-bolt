package ta.nemahuta.neo4j.session.sync;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.javatuples.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
public abstract class AbstractNeo4JElementStateHandler<S extends Neo4JElementState> implements Neo4JElementStateHandler<S> {

    @NonNull
    protected final StatementExecutor statementExecutor;

    @Nonnull
    @Override
    public Map<Long, S> getAll(@Nonnull final Set<Long> idsToBeLoaded) {
        final Map<Long, S> results = new HashMap<>();
        statementExecutor.retrieveRecords(createLoadCommand(idsToBeLoaded)).forEach(r -> {
            final Pair<Long, S> idAndState = getIdAndConvertToState(r);
            results.put(idAndState.getValue0(), idAndState.getValue1());
        });
        return results;
    }

    protected abstract Pair<Long, S> getIdAndConvertToState(final Record r);

    @Override
    public void update(final long id, @Nonnull @NonNull final S currentState, @Nonnull @NonNull final S newState) {
        statementExecutor.executeStatement(createUpdateCommand(id, currentState, newState));
    }

    @Override
    public void delete(final long id) {
        statementExecutor.executeStatement(createDeleteCommand(id));
    }

    @Override
    public long create(@Nonnull final S state) {
        return statementExecutor.retrieveRecords(createInsertCommand(state))
                .findAny()
                .flatMap(r -> r.size() == 1 ? Optional.of(r.get(0)) : Optional.empty())
                .map(Value::asLong)
                .orElseThrow(() -> new IllegalStateException("The statement executed returned a non single long record."));
    }

    /**
     * Create a delete statement to be processed.
     *
     * @param id the id to be deleted
     * @return the statement to be processed
     */
    @Nonnull
    protected abstract Statement createDeleteCommand(long id);

    /**
     * Create an update statement to be processed.
     *
     * @param id           the id of the element to be updated
     * @param currentState
     * @return the statement to be processed
     */
    @Nonnull
    protected abstract Statement createUpdateCommand(long id, final S currentState, S state);

    /**
     * Create an insert statement to be processed.
     *
     * @param state the state to be inserted
     * @return the statement to be processed
     */
    @Nonnull
    protected abstract Statement createInsertCommand(@Nonnull S state);

    /**
     * Create a load command for the provided ids.
     *
     * @param ids the ids to be loaded
     * @return the statement to be processed
     */
    @Nonnull
    protected abstract Statement createLoadCommand(@Nonnull Set<Long> ids);

}
