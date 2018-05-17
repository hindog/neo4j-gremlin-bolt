package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.NonNull;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.types.Node;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JElementIdAdapter;
import ta.nemahuta.neo4j.partition.Neo4JGraphPartition;
import ta.nemahuta.neo4j.property.Neo4JVertexPropertyFactory;
import ta.nemahuta.neo4j.query.vertex.VertexQueryBuilder;
import ta.nemahuta.neo4j.query.vertex.VertexQueryFactory;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link AbstractNeo4JElementScope} for {@link Neo4JVertex}es.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class DefaultNeo4JVertexScope extends AbstractNeo4JElementScope<Neo4JVertex> {

    @Getter(onMethod = @__({@Override, @Nonnull}))
    private final Neo4JVertexPropertyFactory propertyFactory;

    public DefaultNeo4JVertexScope(@Nonnull final Neo4JElementIdAdapter<?> idProvider,
                                   @Nonnull final StatementExecutor statementExecutor,
                                   @Nonnull final Neo4JGraphPartition partition) {
        this(ImmutableMap.of(), idProvider, statementExecutor, partition);
    }

    public DefaultNeo4JVertexScope(@Nonnull final ImmutableMap<Neo4JElementId<?>, Neo4JVertex> initialElements,
                                   @Nonnull final Neo4JElementIdAdapter<?> idProvider,
                                   @Nonnull final StatementExecutor statementExecutor,
                                   @Nonnull @NonNull final Neo4JGraphPartition partition) {
        super(initialElements, idProvider, partition, statementExecutor);
        propertyFactory = new Neo4JVertexPropertyFactory(idAdapter);
    }

    /**
     * @return a new {@link VertexQueryBuilder}
     */
    @Nonnull
    protected VertexQueryBuilder query() {
        return new VertexQueryBuilder(idAdapter, readPartition);
    }

    @Override
    protected Stream<? extends Neo4JElementId<?>> idsWithLabelIn(@Nonnull @NonNull final Set<String> labels) {
        return labels.stream()
                .map(label ->
                        query().match(b -> b.labelsMatch(Collections.singleton(label)))
                                .andThen(b -> b.returnId())
                                .build()
                                .map(statementExecutor::retrieveRecords)
                                .map(records -> records.map(r -> r.get(0).asObject()).map(idAdapter::convert))
                                .orElseGet(Stream::empty)
                )
                .reduce(Stream::concat)
                .orElseGet(Stream::empty);
    }

    @Override
    protected Stream<Neo4JVertex> load(@Nonnull @NonNull final Neo4JGraph graph,
                                       @Nonnull @NonNull final Iterable<? extends Neo4JElementId<?>> ids) {
        return query()
                .match(b -> b.labelsMatch(readPartition.ensurePartitionLabelsSet(Collections.emptySet())))
                .where(b -> b.idsInSet(ImmutableSet.copyOf(ids)))
                .andThen(VertexQueryFactory::returnVertex)
                .build()
                .map(statementExecutor::retrieveRecords)
                .map(records -> records.map(record -> createVertex(graph, record)))
                .orElseGet(Stream::empty);
    }

    @Override
    @Nonnull
    protected Optional<Statement> createDeleteCommand(@Nonnull @NonNull final Neo4JVertex element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .match(b -> b.labelsMatch(committed.getState().labels))
                .where(b -> b.id(committed.getState().id))
                .andThen(VertexQueryFactory::delete)
                .build();
    }

    @Override
    @Nonnull
    protected Optional<Statement> createUpdateCommand(@Nonnull @NonNull final Neo4JVertex element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .match(b -> b.labelsMatch(committed.getState().labels))
                .where(b -> b.id(committed.getState().id))
                .andThen(b -> b.labels(committed.getState().labels, current.getState().labels))
                .andThen(b -> b.properties(committed.getState().properties, current.getState().properties))
                .build();
    }

    @Override
    @Nonnull
    protected Optional<Statement> createInsertCommand(@Nonnull @NonNull final Neo4JVertex element,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> committed,
                                                      @Nonnull @NonNull final StateHolder<Neo4JElementState> current) {
        return query()
                .andThen(b -> b.create(current.getState().id, current.getState().labels, current.getState().properties))
                .build();
    }

    /**
     * Factors a {@link Neo4JVertex} from the provided parameters.
     *
     * @param graph  the graph the edge should be factored for
     * @param record the record to pull the information from
     * @return the factored edge
     */
    @Nonnull
    private Neo4JVertex createVertex(@Nonnull @NonNull final Neo4JGraph graph,
                                     @Nonnull @NonNull final Record record) {

        final Node node = record.get(0).asNode();
        final Neo4JElementId<?> elementId = getIdAdapter().retrieveId(node);
        return new Neo4JVertex(graph, elementId, ImmutableSet.copyOf(node.labels()), Optional.of(node),
                propertyFactory, graph.getSession().inEdgeProviderFactory(),
                graph.getSession().outEdgeProviderFactory(), graph.getSession());
    }
}
