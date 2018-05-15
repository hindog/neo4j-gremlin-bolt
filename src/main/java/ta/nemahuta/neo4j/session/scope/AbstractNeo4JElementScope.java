package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import ta.nemahuta.neo4j.async.AsyncAccess;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;
import ta.nemahuta.neo4j.id.PropertyElementIdGenerator;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Abstract {@link Neo4JElementScope} using an asnychronous access wrapper.
 *
 * @param <T> the type of the element
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public abstract class AbstractNeo4JElementScope<T extends Neo4JElement> implements Neo4JElementScope<T> {

    /**
     * the elements of the scope
     */
    @Nonnull
    protected final AsyncAccess<ImmutableMap<Neo4JElementId<?>, T>> elements;

    /**
     * the {@link Neo4JElementIdAdapter} to be used to access the identifiers for elements
     */
    @Getter(onMethod = @__({@Override, @Nonnull}))
    protected final Neo4JElementIdAdapter<?> idAdapter;

    /**
     * the {@link Neo4JElementIdGenerator} for properties of elements
     */
    @Getter(onMethod = @__({@Override, @Nonnull}))
    protected final Neo4JElementIdGenerator<?> propertyIdGenerator = new PropertyElementIdGenerator();

    /**
     * the executor being used to execute statements
     */
    protected final StatementExecutor statementExecutor;

    @Getter(onMethod = @__({@Override, @Nonnull}))
    protected final Neo4JGraphPartition readPartition;

    /**
     * Create a new scope using initial elements.
     *
     * @param initialElements   the initial elements
     * @param idAdapter         the identifier adapter
     * @param readPartition     the partition being used in the scope
     * @param statementExecutor the executor of the statements
     */
    public AbstractNeo4JElementScope(@Nonnull @NonNull final ImmutableMap<Neo4JElementId<?>, T> initialElements,
                                     @Nonnull @NonNull final Neo4JElementIdAdapter<?> idAdapter,
                                     @Nonnull @NonNull final Neo4JGraphPartition readPartition,
                                     @Nonnull @NonNull final StatementExecutor statementExecutor) {
        this.elements = new AsyncAccess<>(initialElements);
        this.idAdapter = idAdapter;
        this.readPartition = readPartition;
        this.statementExecutor = statementExecutor;
    }

    @Override
    public void add(@Nonnull @NonNull final T element) {
        log.debug("Adding element to scope: {}", element);
        elements.update(e -> ImmutableMap.<Neo4JElementId<?>, T>builder().putAll(e).put(element.id(), element).build());
    }

    @Override
    public void commit() {
        log.debug("Committing elements...");
        invokeRemovingDiscarded(this::commit);
    }

    @Override
    public void rollback() {
        log.debug("Rolling back elements...");
        invokeRemovingDiscarded(e -> e.getState().rollback());
    }

    @Override
    public Stream<T> getOrLoad(@Nonnull @NonNull final Neo4JGraph graph,
                               @Nonnull @NonNull final Iterator<? extends Neo4JElementId<?>> ids) {

        final List<? extends Neo4JElementId<?>> idList = ImmutableList.copyOf(ids);
        final Map<Neo4JElementId<?>, T> loadedMap = new HashMap<>();
        log.debug("Loading {} item(s)...", idList.size());
        elements.update(loaded -> {
            // This builds the new session scope
            final ImmutableMap.Builder<Neo4JElementId<?>, T> resultBuilder = ImmutableMap.builder();
            resultBuilder.putAll(loaded);

            final Set<Neo4JElementId<?>> idsToBeLoaded = new HashSet<>();
            // Iterate through all ids to retrieveId them from the scope
            for (final Neo4JElementId<?> id : idList) {
                final T elemFromScope = loaded.get(id);
                if (elemFromScope == null) {
                    // If we have the id in the scope add it to the loadedMap
                    loadedMap.put(id, elemFromScope);
                } else {
                    // Otherwise we nee to load it
                    idsToBeLoaded.add(id);
                }
            }
            log.debug("Found {} item(s) already in the scope, loading another {}", loadedMap.size(), idsToBeLoaded.size());
            // Now for all the remaining: load them and store them to the new session scope
            load(graph, idsToBeLoaded).forEach(elemFromLoad -> {
                loadedMap.put(elemFromLoad.id(), elemFromLoad);
                resultBuilder.put(elemFromLoad.id(), elemFromLoad);
            });

            return resultBuilder.build();
        });
        return idList.stream().filter(Objects::nonNull).map(loadedMap::get);
    }

    @Override
    public Stream<T> getOrLoadLabelIn(@Nonnull @NonNull final Neo4JGraph graph,
                                      @Nonnull @NonNull final Iterable<String> labels) {
        return getOrLoad(graph, idsWithLabelIn(ImmutableSet.copyOf(labels)).iterator());
    }

    @Override
    public void flush() {
        elements.update(es -> ImmutableMap.of());
    }

    /**
     * Query the ids for each element which has at least one label from the provided orLabelsAnd
     *
     * @param labels the orLabelsAnd to match
     * @return the stream of the ids
     */
    protected abstract Stream<? extends Neo4JElementId<?>> idsWithLabelIn(final Set<String> labels);

    /**
     * Load the records with the provided ids and construct the elements for the provided graph.
     *
     * @param graph the root graph for the elements
     * @param ids   the identifiers for the elements to be loaded
     * @return a stream of the found and loaded elements
     */
    protected abstract Stream<T> load(@Nonnull Neo4JGraph graph,
                                      @Nonnull Iterable<? extends Neo4JElementId<?>> ids);

    /**
     * Create a delete statement to be processed.
     *
     * @param element   the element of the command
     * @param committed the {@link StateHolder} for the committed state of the element
     * @param current   the {@link StateHolder} for the current state of the element
     * @return an optional statement to be processed
     */
    @Nonnull
    protected abstract Optional<Statement> createDeleteCommand(@Nonnull T element, @Nonnull StateHolder<Neo4JElementState> committed, @Nonnull StateHolder<Neo4JElementState> current);

    /**
     * Create an update statement to be processed.
     *
     * @param element   the element of the command
     * @param committed the {@link StateHolder} for the committed state of the element
     * @param current   the {@link StateHolder} for the current state of the element
     * @return an optional statement to be processed
     */
    @Nonnull
    protected abstract Optional<Statement> createUpdateCommand(@Nonnull T element, @Nonnull StateHolder<Neo4JElementState> committed, @Nonnull StateHolder<Neo4JElementState> current);

    /**
     * Create an insert statement to be processed.
     *
     * @param element   the element of the command
     * @param committed the {@link StateHolder} for the committed state of the element
     * @param current   the {@link StateHolder} for the current state of the element
     * @return an optional statement to be processed
     */
    @Nonnull
    protected abstract Optional<Statement> createInsertCommand(@Nonnull T element, @Nonnull StateHolder<Neo4JElementState> committed, @Nonnull StateHolder<Neo4JElementState> current);

    /**
     * Create an optional statement for the provided element and states.
     *
     * @param element   the element
     * @param committed the committed state
     * @param current   the current state
     * @return the optional statement
     */
    @Nonnull
    protected Optional<Statement> maybeCreateStatement(@Nonnull @NonNull final T element,
                                                       @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                       @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        switch (current.syncState) {
            case TRANSIENT:
                return createInsertCommand(element, committed, current);
            case MODIFIED:
                return createUpdateCommand(element, committed, current);
            case DELETED:
                return createDeleteCommand(element, committed, current);
            case DISCARDED:
            case SYNCHRONOUS:
            default:
                return Optional.empty();
        }
    }

    /**
     * Extract the id of the {@link StatementResult}.
     *
     * @param result the statement result
     * @return the optional {@link Neo4JElementId} of the element
     */
    @Nonnull
    protected Optional<Neo4JElementId<?>> extractId(@Nonnull final StatementResult result) {
        if (result.hasNext()) {
            return Optional.of(idAdapter.convert(result.next().get(0).asObject()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Invoke a {@link Consumer} for updating the current elements. Removes all elements which are in state {@link ta.nemahuta.neo4j.state.SyncState#DISCARDED}.
     *
     * @param elementConsumer the consumer of the elements
     */
    protected void invokeRemovingDiscarded(@Nonnull @NonNull final Consumer<T> elementConsumer) {
        elements.update(current -> {
            current.values().forEach(elementConsumer);
            final ImmutableMap.Builder<Neo4JElementId<?>, T> newElements = ImmutableMap.builder();
            current.values().stream()
                    .filter(Neo4JElement::isNotDiscarded)
                    .forEach(e -> newElements.put(e.id(), e));
            return newElements.build();
        });
    }

    /**
     * Commits a single element.
     *
     * @param element the element to be committed.
     */
    protected void commit(@Nonnull @NonNull final T element) {
        element.getState().commit((committed, current) ->
                maybeCreateStatement(element, committed, current)
                        .map(stmt -> logStatement(element, stmt))
                        .map(statementExecutor::executeStatement)
                        .flatMap(this::extractId)
                        .map(current.getState()::withId)
                        .orElse(current.getState())
        );
    }

    /**
     * Logs a {@link Statement} for an {@link Neo4JElement} and returns it.
     *
     * @param element the element the statement should be logged for
     * @param stmt    the statement to be used for logging
     * @return the statement
     */
    @Nonnull
    private Statement logStatement(@Nonnull @NonNull final T element, @Nonnull @NonNull final Statement stmt) {
        log.debug("Created statement for element {}\n\t{}\n\t{}", element, stmt.text(), stmt.parameters());
        return stmt;
    }

}
