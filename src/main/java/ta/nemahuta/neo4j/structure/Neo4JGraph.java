package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.NonNull;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.neo4j.driver.v1.Session;
import ta.nemahuta.neo4j.cache.SessionCache;
import ta.nemahuta.neo4j.cache.SessionCacheManager;
import ta.nemahuta.neo4j.config.Neo4JConfiguration;
import ta.nemahuta.neo4j.features.Neo4JFeatures;
import ta.nemahuta.neo4j.handler.DefaultRelationProvider;
import ta.nemahuta.neo4j.handler.Neo4JEdgeStateHandler;
import ta.nemahuta.neo4j.handler.Neo4JVertexStateHandler;
import ta.nemahuta.neo4j.handler.RelationProvider;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.scope.DefaultNeo4JElementStateScope;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.session.LazyEdgeProvider;
import ta.nemahuta.neo4j.session.Neo4JTransaction;
import ta.nemahuta.neo4j.state.Neo4JEdgeState;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import javax.annotation.Nonnull;
import java.util.*;
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
    private final DefaultNeo4JElementStateScope<Neo4JVertexState> vertexScope;
    private final DefaultNeo4JElementStateScope<Neo4JEdgeState> edgeScope;
    private final Neo4JConfiguration configuration;

    private final SoftRefMap<Long, Neo4JVertex> vertices = new SoftRefMap<>();
    private final SoftRefMap<Long, Neo4JEdge> edges = new SoftRefMap<>();

    private final RelationProvider relationProvider;

    public Neo4JGraph(@Nonnull @NonNull final Session session,
                      @Nonnull @NonNull final SessionCacheManager sessionCacheManager,
                      @Nonnull @NonNull final Neo4JConfiguration configuration) {
        this(session, sessionCacheManager.createSessionCache(session.hashCode()),
                Optional.ofNullable(configuration.getGraphName()).map(Neo4JLabelGraphPartition::allLabelsOf).orElseGet(Neo4JLabelGraphPartition::anyLabel),
                configuration);
    }

    public Neo4JGraph(final Session session,
                      final SessionCache sessionCache,
                      final Neo4JGraphPartition partition, final Neo4JConfiguration configuration) {
        this.session = session;
        this.cache = sessionCache;
        this.partition = partition;
        this.transaction = new Neo4JTransaction(this, session, sessionCache);
        this.relationProvider = new DefaultRelationProvider(transaction, partition);
        this.vertexScope = new DefaultNeo4JElementStateScope<>(sessionCache.getVertexCache(), new Neo4JEdgeStateHandler(transaction, partition));
        this.edgeScope = new DefaultNeo4JElementStateScope<>(sessionCache.getEdgeCache(), new Neo4JVertexStateHandler(transaction, partition));
        this.configuration = configuration;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final ImmutableSet<String> labels = ImmutableSet.copyOf(
                partition.ensurePartitionLabelsSet(
                        Arrays.asList(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(Neo4JVertex.LABEL_DELIMITER))
                )
        );
        final ImmutableMap<String, Object> properties =
                ImmutableMap.copyOf(Maps.filterKeys(ElementHelper.asMap(keyValues), k -> !Objects.equals(T.label, k)));

        final long id = vertexScope.create(new Neo4JVertexState(labels, properties));

        return getOrCreateVertex(id, true);
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
    public Iterator<Vertex> vertices(@Nonnull @NonNull final Object... vertexIds) {
        return loadAndReturnFoundElementsOnly(vertexScope, id -> getOrCreateVertex(id, false), vertexIds);
    }

    @Override
    public Iterator<Edge> edges(@Nonnull @NonNull final Object... edgeIds) {
        return loadAndReturnFoundElementsOnly(edgeScope, id -> getOrCreateEdge(id), edgeIds);
    }

    private <R, S extends Neo4JElementState> Iterator<R> loadAndReturnFoundElementsOnly(@Nonnull @NonNull final Neo4JElementStateScope<S> scope,
                                                                                        @Nonnull @NonNull final Function<Long, R> accessor,
                                                                                        @Nonnull @NonNull final Object... ids) {
        // Load all elements using the scope
        final Map<Long, S> loaded = scope.getAll(ImmutableList.copyOf(Stream.of(ids).filter(l -> l instanceof Long).map(l -> (Long) l).iterator()));
        // Stream the ids and return those who had long ids and have been found only
        return Stream.of(ids)
                .map(id -> (id instanceof Long) && loaded.get(id) == null ? accessor.apply((Long) id) : null).iterator();
    }

    Neo4JEdge addEdge(final String label, final Neo4JVertex outVertex, final Neo4JVertex inVertex, final Object... keyValues) {

        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (!(inVertex instanceof Neo4JVertex)) {
            throw new IllegalArgumentException("Cannot handle a vertex of type: " + inVertex.getClass().getName());
        }

        final ImmutableMap<String, Object> properties =
                ImmutableMap.copyOf(Maps.filterKeys(ElementHelper.asMap(keyValues), k -> !Objects.equals(T.label, k)));

        final long id = edgeScope.create(new Neo4JEdgeState(label, properties, inVertex.id(), outVertex.id()));

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
        cache.close();
        transaction.close();
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

    private Vertex getOrCreateVertex(final long id, final boolean justCreated) {
        return vertices.getOrCreate(id, () -> {
            final EdgeProvider inEdgeProvider = new LazyEdgeProvider(labels -> relationProvider.loadRelatedIds(id, Direction.IN, labels), justCreated);
            final EdgeProvider outEdgeProvider = new LazyEdgeProvider(labels -> relationProvider.loadRelatedIds(id, Direction.OUT, labels), justCreated);
            final Neo4JVertex newVertex = new Neo4JVertex(this, id, vertexScope, inEdgeProvider, outEdgeProvider);
            return newVertex;
        });
    }

    private Neo4JEdge getOrCreateEdge(final long id) {
        return edges.getOrCreate(id, () -> new Neo4JEdge(this, id, edgeScope));
    }
}
