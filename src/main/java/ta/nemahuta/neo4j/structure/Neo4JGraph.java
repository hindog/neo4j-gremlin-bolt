package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.neo4j.driver.v1.Session;
import ta.nemahuta.neo4j.cache.HierarchicalCache;
import ta.nemahuta.neo4j.cache.SessionCache;
import ta.nemahuta.neo4j.cache.SessionCacheManager;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.features.Neo4JFeatures;
import ta.nemahuta.neo4j.handler.DefaultRelationHandler;
import ta.nemahuta.neo4j.handler.Neo4JEdgeStateHandler;
import ta.nemahuta.neo4j.handler.Neo4JVertexStateHandler;
import ta.nemahuta.neo4j.handler.RelationHandler;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.query.AbstractQueryBuilder;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.scope.DefaultNeo4JElementStateScope;
import ta.nemahuta.neo4j.scope.IdCache;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.session.Neo4JTransaction;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;
import ta.nemahuta.neo4j.state.VertexEdgeReferences;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The Neo4J implementation for a {@link Graph}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@GraphFactoryClass(Neo4JGraphFactory.class)
public class Neo4JGraph implements Graph {

    private final Session session;

    private final Neo4JTransaction transaction;

    private final SessionCache cache;

    private final Neo4JGraphPartition partition;
    private final DefaultNeo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder> vertexScope;
    private final DefaultNeo4JElementStateScope<Neo4JEdgeState, EdgeQueryBuilder> edgeScope;
    private final Neo4JConfiguration configuration;

    private final SoftRefMap<Long, Neo4JVertex> vertices = new SoftRefMap<>();
    private final SoftRefMap<Long, Neo4JEdge> edges = new SoftRefMap<>();

    private final RelationHandler relationHandler;
    private final Neo4JVertexStateHandler vertexStateHandler;
    private final Neo4JEdgeStateHandler edgeStateHandler;

    public Neo4JGraph(@Nonnull final Session session,
                      @Nonnull final SessionCacheManager sessionCacheManager,
                      @Nonnull final Neo4JConfiguration configuration) {
        this(session, sessionCacheManager.createSessionCache(session.hashCode()),
                Optional.ofNullable(configuration.getGraphName()).map(Neo4JLabelGraphPartition::allLabelsOf).orElseGet(Neo4JLabelGraphPartition::anyLabel),
                configuration);
    }

    public Neo4JGraph(final Session session,
                      final SessionCache sessionCache,
                      final Neo4JGraphPartition partition,
                      final Neo4JConfiguration configuration) {
        this.session = session;
        this.cache = sessionCache;
        this.partition = partition;
        this.transaction = new Neo4JTransaction(this, session);
        this.transaction.addTransactionListener(this::handleTransaction);
        this.vertexStateHandler = new Neo4JVertexStateHandler(transaction, partition);
        this.edgeStateHandler = new Neo4JEdgeStateHandler(transaction, partition);
        this.edgeScope = new DefaultNeo4JElementStateScope<>(sessionCache.getEdgeCache(), edgeStateHandler, sessionCache.getKnownEdgeIds());
        final HierarchicalCache<Long, Neo4JVertexState> vertexCache = sessionCache.getVertexCache();
        final IdCache<Long> knownVertexIds = sessionCache.getKnownVertexIds();
        this.vertexScope = new DefaultNeo4JElementStateScope<Neo4JVertexState, VertexQueryBuilder>(vertexCache, vertexStateHandler, knownVertexIds) {
            @Override
            public void delete(long id) {
                final Set<Long> edgeIds = knownVertexIds.getRemoved().contains(id)
                        // For a removed vertex, the referenced edges are empty
                        ? ImmutableSet.of()
                        // For all others, the referenced edges can be obtained by using the state's in and outgoing references
                        : Optional.ofNullable(vertexCache.get(id))
                        .map(state ->
                                Stream.concat(state.getIncomingEdgeIds().getAllKnown(), state.getOutgoingEdgeIds().getAllKnown())
                                        .collect(ImmutableSet.toImmutableSet()))
                        .orElseGet(() -> ImmutableSet.of());
                // Make sure the known edges are marked deleted, so the cache is not out of sync
                edgeIds.forEach(edgeScope::delete);
                // Make sure to remove all the references for those edges in all the states
                vertexCache.getKeys().forEach(key -> {
                    Optional.ofNullable(vertexCache.get(key)).ifPresent(state -> {
                        final Neo4JVertexState newState = state.withRemovedEdges(edgeIds);
                        if (newState != state) {
                            vertexCache.put(key, newState);
                        }
                    });
                });
                // Finally delegate the deletion
                super.delete(id);
            }
        };
        this.relationHandler = new DefaultRelationHandler(vertexScope, sessionCache.getVertexCache(), edgeScope);
        this.configuration = configuration;
    }

    private void handleTransaction(final Transaction.Status status) {
        switch (status) {
            case COMMIT:
                this.edgeScope.commit();
                this.vertexScope.commit();
                break;
            case ROLLBACK:
                this.edgeScope.rollback();
                this.vertexScope.rollback();
                break;
        }
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final ImmutableSet<String> labels = ImmutableSet.copyOf(
                partition.ensurePartitionLabelsNotSet(
                        Arrays.asList(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(Neo4JVertex.LABEL_DELIMITER))
                )
        );
        final ImmutableMap<String, Object> properties =
                ImmutableMap.copyOf(Maps.filterKeys(ElementHelper.asMap(keyValues), k -> !Objects.equals("label", k)));

        final long id = vertexScope.create(new Neo4JVertexState(labels, properties,
                new VertexEdgeReferences().withAllResolvedEdges(Collections.emptyMap()),
                new VertexEdgeReferences().withAllResolvedEdges(Collections.emptyMap())
        ));

        return getOrCreateVertex(id);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(@Nonnull final Object... vertexIds) {
        return loadAndReturnFoundElementsOnly(vertexScope, id -> getOrCreateVertex(id), vertexIds);
    }

    @Override
    public Iterator<Edge> edges(@Nonnull final Object... edgeIds) {
        return loadAndReturnFoundElementsOnly(edgeScope, id -> getOrCreateEdge(id), edgeIds);
    }

    /**
     * Creates an index for a vertex property using the provided labels to match them.
     *
     * @param labels       the labels of the vertices to match
     * @param propertyName the name of the property to create an index for
     */
    public void createVertexPropertyIndex(@Nonnull final Set<String> labels,
                                          @Nonnull final String propertyName) {
        vertexStateHandler.createIndex(labels, propertyName);
    }

    /**
     * Creates an index for a edge property using the provided label to match them.
     *
     * @param label        the labels of the vertices to match
     * @param propertyName the name of the property to create an index for
     */
    public void createEdgePropertyIndex(@Nonnull final String label,
                                        @Nonnull final String propertyName) {
        edgeStateHandler.createIndex(Collections.singleton(label), propertyName);
    }

    private <R, S extends Neo4JElementState> Iterator<R> loadAndReturnFoundElementsOnly(@Nonnull final Neo4JElementStateScope<S, ? extends AbstractQueryBuilder> scope,
                                                                                        @Nonnull final Function<Long, R> accessor,
                                                                                        @Nonnull final Object... ids) {
        // Load all elements using the scope
        final Collection<Long> idCollection = Stream.of(ids).filter(Long.class::isInstance).map(l -> (Long) l).collect(ImmutableList.toImmutableList());
        final Map<Long, S> loaded = scope.getAll(ImmutableList.copyOf(idCollection.iterator()));
        // Stream the ids and return those who had long ids and have been found only

        return (!idCollection.isEmpty() ? idCollection.stream() : loaded.keySet().stream())
                .filter(loaded::containsKey)
                .map(accessor::apply)
                .iterator();
    }

    Neo4JEdge addEdge(@Nonnull final String label,
                      @Nonnull final Neo4JVertex outVertex,
                      @Nonnull final Neo4JVertex inVertex, final Object... keyValues) {

        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final ImmutableMap<String, Object> properties =
                ImmutableMap.copyOf(Maps.filterKeys(ElementHelper.asMap(keyValues), k -> !Objects.equals(T.label, k)));

        final long id = edgeScope.create(new Neo4JEdgeState(label, properties, inVertex.id(), outVertex.id()));
        relationHandler.registerEdge(outVertex.id, Direction.OUT, label, id);
        relationHandler.registerEdge(inVertex.id, Direction.IN, label, id);
        return getOrCreateEdge(id);
    }

    @Override
    public Transaction tx() {
        return transaction;
    }

    @Override
    public void close() {
        if (transaction.isOpen()) {
            throw Transaction.Exceptions.openTransactionsOnClose();
        }
        session.close();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration.toApacheConfiguration();
    }

    @Override
    public Features features() {
        return Neo4JFeatures.INSTANCE;
    }

    private Vertex getOrCreateVertex(final long id) {
        return vertices.getOrCreate(id, () -> {
            final Neo4JVertex newVertex = new Neo4JVertex(this, id, vertexScope, relationHandler);
            return newVertex;
        });
    }

    private Neo4JEdge getOrCreateEdge(final long id) {
        return edges.getOrCreate(id, () -> new Neo4JEdge(this, id, edgeScope));
    }
}
