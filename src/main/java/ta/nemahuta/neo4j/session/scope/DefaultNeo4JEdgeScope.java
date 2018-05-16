package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.Relationship;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.property.Neo4JEdgePropertyFactory;
import ta.nemahuta.neo4j.query.edge.EdgeQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JVertex;
import ta.nemahuta.neo4j.structure.VertexOnEdgeSupplier;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link AbstractNeo4JElementScope} for {@link Neo4JEdge}es.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class DefaultNeo4JEdgeScope extends AbstractNeo4JElementScope<Neo4JEdge> implements Neo4JEdgeScope {

    /**
     * the {@link Neo4JElementScope} for {@link Neo4JVertex}es
     */
    private final Neo4JElementScope<Neo4JVertex> vertexScope;

    @Getter(onMethod = @__({@Override, @Nonnull}))
    private final Neo4JEdgePropertyFactory propertyFactory;

    public DefaultNeo4JEdgeScope(@Nonnull @NonNull final Neo4JElementIdAdapter<?> idProvider,
                                 @Nonnull @NonNull final StatementExecutor statementExecutor,
                                 @Nonnull @NonNull final Neo4JGraphPartition partition,
                                 @Nonnull @NonNull final Neo4JElementScope<Neo4JVertex> vertexScope) {
        this(ImmutableMap.of(), idProvider, statementExecutor, partition, vertexScope);
    }

    public DefaultNeo4JEdgeScope(@Nonnull @NonNull final ImmutableMap<Neo4JElementId<?>, Neo4JEdge> initialElements,
                                 @Nonnull @NonNull final Neo4JElementIdAdapter<?> idProvider,
                                 @Nonnull @NonNull final StatementExecutor statementExecutor,
                                 @Nonnull @NonNull final Neo4JGraphPartition partition,
                                 @Nonnull @NonNull final Neo4JElementScope<Neo4JVertex> vertexScope) {
        super(initialElements, idProvider, partition, statementExecutor);
        this.vertexScope = vertexScope;
        propertyFactory = new Neo4JEdgePropertyFactory(idAdapter);
    }

    /**
     * @return a new {@link VertexQueryBuilder}
     */
    @Nonnull
    protected EdgeQueryBuilder query() {
        return new EdgeQueryBuilder(idAdapter, readPartition, vertexScope.getIdAdapter());
    }

    @Override
    public Stream<Neo4JEdge> inEdgesOf(@Nonnull @NonNull final Neo4JGraph graph,
                                       @Nonnull @NonNull final Neo4JVertex v,
                                       @Nonnull @NonNull final Iterable<String> labels) {
        return edgesOf(graph, v, labels, Direction.IN);
    }

    @Override
    public Stream<Neo4JEdge> outEdgesOf(@Nonnull @NonNull final Neo4JGraph graph,
                                        @Nonnull @NonNull final Neo4JVertex v,
                                        @Nonnull @NonNull final Iterable<String> labels) {
        return edgesOf(graph, v, labels, Direction.OUT);
    }


    @Override
    @Nonnull
    protected Stream<? extends Neo4JElementId<?>> idsWithLabelIn(@Nonnull @NonNull final Set<String> labels) {
        return idsWithLabelIn(labels, Stream.of(Direction.IN, Direction.OUT));
    }

    /**
     * Query all {@link Neo4JElementId}s for which the edges have at least one of the provided labels and one of the provided directions.
     *
     * @param labels     the labels to be used to match
     * @param directions the directions to be used
     * @return the {@link Stream} which provides the {@link Neo4JElementId}s
     */
    @Nonnull
    protected Stream<? extends Neo4JElementId<?>> idsWithLabelIn(@Nonnull @NonNull final Set<String> labels,
                                                                 @Nonnull @NonNull final Stream<Direction> directions) {
        return directions.map(
                direction -> query()
                        .direction(direction)
                        .labels(labels)
                        .andThen(b -> b.returnId())
                        .build()
                        .map(statementExecutor::retrieveRecords)
                        .map(records -> records.map(r -> idAdapter.convert(r.get(0).asObject())))
                        .orElseGet(Stream::empty)
        ).reduce(Stream::concat).map(Stream::distinct).orElseGet(Stream::empty);
    }

    @Override
    @Nonnull
    protected Stream<Neo4JEdge> load(@Nonnull @NonNull final Neo4JGraph graph,
                                     @Nonnull @NonNull final Iterable<? extends Neo4JElementId<?>> ids) {
        return query()
                .direction(Direction.OUT)
                .where(b -> b.whereIds(ImmutableSet.copyOf(ids)))
                .andThen(b -> b.returnEdge())
                .build()
                .map(statementExecutor::retrieveRecords)
                .map(records -> records.map(record -> this.createEdge(graph, record)))
                .orElseGet(Stream::empty);
    }

    @Override
    @Nonnull
    protected Optional<Statement> createDeleteCommand(@Nonnull @NonNull final Neo4JEdge element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .direction(Direction.BOTH)
                .labels(Collections.singleton(element.label()))
                .where(b -> b.getLhs().id(element.getInSupplier().getVertexId()).and(b.getRhs().id(element.getOutSupplier().getVertexId())))
                .andThen(b -> b.deleteEdge())
                .build();
    }

    @Override
    @Nonnull
    protected Optional<Statement> createUpdateCommand(@Nonnull @NonNull final Neo4JEdge element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .where(b -> b.getLhs().id(element.getInSupplier().getVertexId()).and(b.getRhs().id(element.getOutSupplier().getVertexId())).and(b.whereId(element.id())))
                .direction(Direction.BOTH)
                .andThen(b -> b.properties(committed.getState().properties, current.getState().properties))
                .build();
    }

    @Override
    @Nonnull
    protected Optional<Statement> createInsertCommand(@Nonnull @NonNull final Neo4JEdge element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .where(b -> b.getLhs().id(element.getInSupplier().getVertexId()).and(b.getRhs().id(element.getOutSupplier().getVertexId())))
                .andThen(b -> b.createEdge(element.id(), Direction.OUT, element.label(), current.getState().properties))
                .build();
    }

    /**
     * Query the edges using the provided parameters.
     *
     * @param graph     the graph the edges should be queried on
     * @param v         the vertex which denotes the lhs of the edge
     * @param labels    the labels to match the edge
     * @param direction the direction for the edge
     * @return the {@link Stream} of {@link Neo4JEdge}s
     */
    protected Stream<Neo4JEdge> edgesOf(@Nonnull @NonNull final Neo4JGraph graph,
                                        @Nonnull @NonNull final Neo4JVertex v,
                                        @Nonnull @NonNull final Iterable<String> labels,
                                        @Nonnull @NonNull final Direction direction) {
        return getOrLoad(graph,
                query()
                        .labels(ImmutableSet.copyOf(labels))
                        .direction(direction)
                        .where(b -> b.getLhs().id(v.id()))
                        .andThen(b -> b.returnId())
                        .build()
                        .map(statementExecutor::retrieveRecords)
                        .map(rs -> rs.map(r -> r.get(0).asObject()).map(idAdapter::convert))
                        .orElseGet(Stream::empty).iterator()
        );
    }

    /**
     * Create a {@link VertexOnEdgeSupplier} which provides a {@link Neo4JVertex} on any side of a {@link Neo4JEdge}.
     *
     * @param graph the graph for the query
     * @param id    the identifier for the query
     * @return the supplier
     */
    @Nonnull
    protected VertexOnEdgeSupplier createVertexGet(@NonNull @Nonnull final Neo4JGraph graph,
                                                   @Nonnull @NonNull final Neo4JElementId<?> id) {
        return VertexOnEdgeSupplier.wrap(
                () -> id,
                () -> vertexScope.getOrLoad(graph, Collections.singleton(id).iterator()).findAny().orElse(null)
        );
    }

    /**
     * Factors a {@link Neo4JEdge} from the provided parameters.
     *
     * @param graph  the graph the edge should be factored for
     * @param record the record to pull the information from
     * @return the factored edge
     */
    @Nonnull
    private Neo4JEdge createEdge(@Nonnull @NonNull final Neo4JGraph graph,
                                 @Nonnull @NonNull final Record record) {

        final Neo4JElementId<?> outId = vertexScope.getIdAdapter().convert(record.get(0).asObject());
        final Relationship relationship = record.get(1).asRelationship();
        final Neo4JElementId<?> inId = vertexScope.getIdAdapter().convert(record.get(2).asObject());
        final Neo4JElementId<?> elementId = getIdAdapter().retrieveId(relationship);

        return new Neo4JEdge(graph, elementId, ImmutableSet.of(relationship.type()), Optional.of(relationship),
                propertyFactory, createVertexGet(graph, inId), createVertexGet(graph, outId));
    }

}
