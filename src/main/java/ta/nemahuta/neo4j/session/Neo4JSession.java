package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableSet;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import ta.nemahuta.neo4j.async.AsyncAccess;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.scope.DefaultNeo4JEdgeScope;
import ta.nemahuta.neo4j.session.scope.DefaultNeo4JVertexScope;
import ta.nemahuta.neo4j.session.scope.DefaultSessionScope;
import ta.nemahuta.neo4j.state.SyncState;
import ta.nemahuta.neo4j.structure.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A wrapper for the Neo4J session which holds the actual {@link Transaction} on the session and is able to execute statements.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@ToString
@EqualsAndHashCode
@Slf4j
public class Neo4JSession implements StatementExecutor, EdgeFactory, AutoCloseable {

    public static final String STMT_PREFIX_PROFILE = "PROFILE ";
    public static final String STMT_PREFIX_EXPLAIN = "EXPLAIN ";

    private final AsyncAccess<Optional<Transaction>> txHolder = new AsyncAccess<>(Optional.empty());

    @Getter
    private final Neo4JGraph graph;
    private final Session wrapped;
    @Getter
    private final SessionScope scope;

    private final boolean profilingEnabled;
    private final Driver driver;

    public Neo4JSession(@Nonnull @NonNull final Driver driver,
                        @Nonnull @NonNull final Neo4JConfiguration configuration) {
        this.driver = driver;
        this.wrapped = driver.session();
        this.profilingEnabled = configuration.isProfilingEnabled();
        this.graph = new Neo4JGraph(this, configuration);
        this.scope = createSessionScope(driver, configuration);
    }

    protected SessionScope createSessionScope(@Nonnull @NonNull final Driver driver,
                                              @Nonnull @NonNull final Neo4JConfiguration configuration) {


        final Neo4JGraphPartition partition = Optional.ofNullable(configuration.getGraphName())
                .map(Neo4JLabelGraphPartition::allLabelsOf)
                .orElseGet(Neo4JLabelGraphPartition::anyLabel);

        final Neo4JElementIdAdapter<?> vertexIdProvider = configuration.createVertexIdAdapter(driver);
        final DefaultNeo4JVertexScope vertexScope = new DefaultNeo4JVertexScope(vertexIdProvider, this, partition);

        final Neo4JElementIdAdapter<?> edgeIdProvider = configuration.createEdgeIdAdapter(driver);

        return new DefaultSessionScope(vertexScope, new DefaultNeo4JEdgeScope(edgeIdProvider, this, partition, vertexScope));
    }

    /**
     * Open the transaction.
     *
     * @throws IllegalStateException in case the transaction is already open
     */
    public void txOpen() {
        txHolder.update(o -> {
            // Make sure the transaction is not already open
            o.ifPresent(t -> {
                throw org.apache.tinkerpop.gremlin.structure.Transaction.Exceptions.transactionAlreadyOpen();
            });
            final Transaction tx = wrapped.beginTransaction();
            log.debug("Created new transaction: {}", tx.hashCode());
            return Optional.of(tx);
        });
    }

    /**
     * Commit and close the transaction.
     *
     * @throws IllegalStateException in case the transaction is not open
     */
    public void txCommit() {
        closeTransaction(Transaction::success, "Committing transaction: {}");

    }

    /**
     * Rollback and close the transaction.
     *
     * @throws IllegalStateException in case the transaction is not open
     */
    public void txRollback() {
        closeTransaction(Transaction::failure, "Rolling back transaction: {}");
    }

    /**
     * Internal implementation, closes the transaction after invoking the provided operation.
     *
     * @param operation the operation to be invoked before closing
     * @param message   the message for logging
     */
    private void closeTransaction(@Nonnull @NonNull final Consumer<Transaction> operation,
                                  @Nonnull @NonNull final String message) {
        txHolder.update(o -> {
            final Transaction tx = o.orElseThrow(Exceptions::transactionNotOpen);
            try {
                log.debug(message, tx.hashCode());
                operation.accept(tx);
            } finally {
                tx.close();
            }
            return Optional.empty();
        });
    }

    /**
     * @return {@code true} if the transaction is open, {@code false} otherwise
     */
    public boolean isTxOpen() {
        return Optional.ofNullable(txHolder.get(Optional::isPresent)).orElse(false);
    }

    @Override
    @Nullable
    public StatementResult executeStatement(@Nonnull @NonNull final Statement statement) {
        return txHolder.getThrows(txOpt -> {
            final Transaction tx = txOpt.orElseThrow(Exceptions::transactionNotOpen);
            try {
                final Statement cypherStatement = wrapIntoProfiling(statement);
                log.debug("Executing Cypher statement on transaction [{}]: {}", tx.hashCode(), cypherStatement.text(), cypherStatement.parameters());
                return Optional.ofNullable(tx.run(cypherStatement)).orElseThrow(Exceptions::txStatementReturnedNull);
            } catch (ClientException ex) {
                if (log.isErrorEnabled())
                    log.error("Error executing Cypher statement on transaction [{}]", tx.hashCode(), ex);
                throw ex;
            }
        });
    }

    /**
     * Wrap the {@link Statement#text} into {@link #STMT_PREFIX_PROFILE} in case profiling is activated.
     *
     * @param original the original statement
     * @return the new statement
     */
    @Nonnull
    private Statement wrapIntoProfiling(@Nonnull @NonNull final Statement original) {
        // Only createEdge the transformation in case profiling is enabled, and the statement has text
        return (!profilingEnabled ? Optional.<String>empty() : Optional.ofNullable(original.text()))
                .map(text -> {
                    final String uppercase = text.toUpperCase();
                    if (uppercase.startsWith(STMT_PREFIX_PROFILE) || uppercase.startsWith(STMT_PREFIX_EXPLAIN)) {
                        // In case the statement is already prefixed with explain or profile, do not modify it
                        return original;
                    } else {
                        return new Statement(STMT_PREFIX_PROFILE + text, original.parameters());
                    }
                }).orElse(original);
    }

    /**
     * Creates a vertex from key value pairs and adds it to the {@link #scope}.
     *
     * @param keyValues the key value pairs
     * @return the new vertex
     */
    public Neo4JVertex createVertex(@Nonnull @NonNull final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final ImmutableSet<String> labels = ImmutableSet.copyOf(
                scope.getVertexScope().getReadPartition().ensurePartitionLabelsSet(
                        Arrays.asList(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(Neo4JVertex.LabelDelimiter))
                )
        );

        final Neo4JVertex vertex = new Neo4JVertex(graph,
                scope.getVertexScope().getIdAdapter().generate(),
                labels,
                Optional.empty(),
                getScope().getVertexScope().getPropertyFactory(),
                inEdgeProviderFactory(), outEdgeProviderFactory(), this::createEdge);

        ElementHelper.attachProperties(vertex, keyValues);
        scope.getVertexScope().add(vertex);
        return vertex;
    }


    @Nonnull
    public Iterator<Vertex> getOrLoadVertices(@Nonnull @NonNull final Neo4JGraph graph,
                                              @Nonnull @NonNull final Object... vertexIds) {
        return scope.getVertexScope()
                .getOrLoad(graph, Arrays.stream(vertexIds).map(scope.getVertexScope().getIdAdapter()::convert).iterator())
                .map(e -> (Vertex) e).iterator();
    }

    @Nonnull
    public Iterator<Edge> getOrLoadEdges(@Nonnull @NonNull final Neo4JGraph graph,
                                         @Nonnull @NonNull final Object... edgeIds) {
        return scope.getEdgeScope()
                .getOrLoad(graph, Arrays.stream(edgeIds).map(scope.getEdgeScope().getIdAdapter()::convert).iterator())
                .map(e -> (Edge) e).iterator();
    }

    /**
     * @return the factory which provides the inbound {@link EdgeProvider} for a given {@link Neo4JVertex}
     */
    public Function<Neo4JVertex, EdgeProvider> inEdgeProviderFactory() {
        return v -> new LazyEdgeProvider(v, labels -> scope.getEdgeScope().inEdgesOf(graph, v, labels), SyncState.TRANSIENT.equals(v.getState().getCurrentSyncState()));
    }

    /**
     * @return the factory which provides the outbound {@link EdgeProvider} for a given {@link Neo4JVertex}
     */
    public Function<Neo4JVertex, EdgeProvider> outEdgeProviderFactory() {
        return v -> new LazyEdgeProvider(v, labels -> scope.getEdgeScope().outEdgesOf(graph, v, labels), SyncState.TRANSIENT.equals(v.getState().getCurrentSyncState()));
    }

    @Nonnull
    @Override
    public Neo4JEdge createEdge(@Nonnull final String label,
                                @Nonnull final Neo4JVertex outVertex,
                                @Nonnull final Vertex inVertex,
                                @Nonnull final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (!(inVertex instanceof Neo4JVertex)) {
            throw new IllegalArgumentException("Cannot handle a vertex of type: " + inVertex.getClass().getName());
        }

        final Neo4JEdge edge = new Neo4JEdge(graph,
                getScope().getEdgeScope().getIdAdapter().generate(),
                ImmutableSet.of(label),
                Optional.empty(),
                getScope().getEdgeScope().getPropertyFactory(),
                VertexOnEdgeSupplier.wrap((Neo4JVertex) inVertex),
                VertexOnEdgeSupplier.wrap(outVertex)
        );

        ElementHelper.attachProperties(edge, keyValues);
        scope.getEdgeScope().add(edge);
        return edge;
    }

    @Override
    public void close() throws Exception {
        if (isTxOpen()) {
            throw new IllegalStateException("Transaction is still open, refusing to close the session.");
        }
        if (wrapped.isOpen()) {
            wrapped.close();
        }
        scope.flush();
        driver.close();
    }


    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Exceptions {

        public static IllegalStateException transactionNotOpen() {
            return new IllegalStateException("Transaction is not open");
        }

        public static IllegalStateException txStatementReturnedNull() {
            return new IllegalStateException("Driver returned null as statement execution result");
        }
    }

}
