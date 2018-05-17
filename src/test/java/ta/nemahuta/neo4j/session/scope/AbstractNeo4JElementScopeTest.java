package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;
import ta.nemahuta.neo4j.query.TestNeo4JPropertyFactory;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JGraph;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ta.nemahuta.neo4j.testutils.MockUtils.mockMapAccessor;

@ExtendWith(MockitoExtension.class)
class AbstractNeo4JElementScopeTest {

    private final Neo4JGraph graph = mock(Neo4JGraph.class);
    private Neo4JElement elemSync, elemTransient, elemRemote;

    @Mock
    private Statement deleteStmt, updateStmt, insertStmt;

    @Mock
    private StatementExecutor executor;

    private AbstractNeo4JElementScope<Neo4JElement> sut;
    private TestNeo4JPropertyFactory propertyFactory;

    private Set<String> lastLabels;
    private Neo4JGraph lastGraph;
    private Iterable<? extends Neo4JElementId<?>> lastIds;

    @BeforeEach
    void createSutAndStub() {
        this.propertyFactory = new TestNeo4JPropertyFactory();
        this.elemSync = element(new Neo4JPersistentElementId<>(1l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b"));
        this.elemTransient = element(new Neo4JTransientElementId<>(1l), ImmutableSet.of("x", "y"), ImmutableMap.of());
        this.elemRemote = element(new Neo4JPersistentElementId<>(3l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b"));

        this.sut = new AbstractNeo4JElementScope<Neo4JElement>(ImmutableMap.of(elemSync.id(), elemSync), new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.anyLabel(), executor) {
            @Override
            public AbstractPropertyFactory<? extends Neo4JProperty<Neo4JElement, ?>> getPropertyFactory() {
                return propertyFactory;
            }

            @Override
            protected Stream<? extends Neo4JElementId<?>> idsWithLabelIn(final Set<String> labels) {
                lastLabels = labels;
                return Stream.of(elemRemote.id(), elemSync.id());
            }

            @Override
            protected Stream<Neo4JElement> load(@Nonnull final Neo4JGraph graph, @Nonnull final Iterable<? extends Neo4JElementId<?>> ids) {
                lastGraph = graph;
                lastIds = ids;
                return Stream.of(elemRemote);
            }

            @Nonnull
            @Override
            protected Optional<Statement> createDeleteCommand(@Nonnull final Neo4JElement element, @Nonnull final StateHolder<Neo4JElementState> committed, @Nonnull final StateHolder<Neo4JElementState> current) {
                return Optional.of(deleteStmt);
            }

            @Nonnull
            @Override
            protected Optional<Statement> createUpdateCommand(@Nonnull final Neo4JElement element, @Nonnull final StateHolder<Neo4JElementState> committed, @Nonnull final StateHolder<Neo4JElementState> current) {
                return Optional.of(updateStmt);
            }

            @Nonnull
            @Override
            protected Optional<Statement> createInsertCommand(@Nonnull final Neo4JElement element, @Nonnull final StateHolder<Neo4JElementState> committed, @Nonnull final StateHolder<Neo4JElementState> current) {
                return Optional.of(insertStmt);
            }
        };
    }

    @Test
    void add() {
        // when: 'adding an element to the scope'
        sut.add(elemTransient);
        // then: 'the element has been put to the map correctly'
        sut.elements.consume(es -> {
            assertTrue(es.containsKey(elemTransient.id()));
            assertTrue(es.containsKey(elemSync.id()));
        });
    }

    @Test
    void commitSynchronous() {
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the insert statement was never called'
        verify(executor, never()).executeStatement(any());
        assertEquals(SyncState.SYNCHRONOUS, elemSync.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertTrue(c.containsKey(elemSync.id())));
    }

    @Test
    void commitTransient() {
        // setup: 'a deleted element'
        sut.add(elemTransient);
        final StatementResult stmtResult = mockRecordWithId(4l);
        when(executor.executeStatement(insertStmt)).thenReturn(stmtResult);
        final Neo4JElementId<?> previousId = elemTransient.id();
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, times(1)).executeStatement(insertStmt);
        assertEquals(SyncState.SYNCHRONOUS, elemTransient.getState().getCurrentSyncState());
        sut.elements.consume(c -> {
            assertTrue(c.containsKey(previousId));
            assertTrue(c.containsKey(elemTransient.id()));
        });
    }

    @Test
    void commitModified() {
        // setup: 'a deleted element'
        elemSync.getState().modify(s -> s.withLabels(ImmutableSet.of("y", "z")));
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, times(1)).executeStatement(updateStmt);
        assertEquals(SyncState.SYNCHRONOUS, elemSync.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertTrue(c.containsKey(elemSync.id())));
    }

    @Test
    void commitDeleted() {
        // setup: 'a deleted element'
        elemSync.remove();
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, times(1)).executeStatement(deleteStmt);
        assertEquals(SyncState.DISCARDED, elemSync.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertFalse(c.containsKey(elemSync.id())));
    }

    @Test
    void commitDiscarded() {
        // setup: 'a discarded element'
        elemTransient.remove();
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, never()).executeStatement(any());
        assertEquals(SyncState.DISCARDED, elemTransient.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertFalse(c.containsKey(elemTransient.id())));
    }

    @Test
    void rollback() {
        // setup: 'a transient element'
        sut.add(elemTransient);
        final ImmutableSet<String> labels = elemSync.getState().current(s -> s.labels);
        final ImmutableMap<String, ? extends Neo4JProperty<? extends Neo4JElement, ?>> props = elemSync.getState().current(s -> s.properties);
        // when: 'modifying the synchronous element'
        elemSync.getState().modify(s -> s.withLabels(ImmutableSet.of()));
        // and: 'rolling back'
        sut.rollback();
        // then: 'no statement was executed'
        verify(executor, never()).executeStatement(any());
        assertEquals(SyncState.DISCARDED, elemTransient.getState().getCurrentSyncState());
        assertEquals(labels, elemSync.getState().current(s -> s.labels));
        assertEquals(props, elemSync.getState().current(s -> s.properties));
        sut.elements.consume(c -> {
            assertTrue(c.containsKey(elemSync.id()));
            assertFalse(c.containsKey(elemTransient.id()));
        });

    }

    @Test
    void getOrLoad() {
        // when: 'loading the synchronous and remote element'
        final Stream<Neo4JElement> actual = sut.getOrLoad(graph, Stream.of(elemSync.id(), elemRemote.id()).iterator());
        // then: 'the synchronous element and the remote element are returned'
        assertEquals(ImmutableSet.of(elemSync, elemRemote), actual.collect(Collectors.toSet()));
        // and: 'the last ids loaded only contain the remote element and the graph was used'
        assertEquals(graph, lastGraph);
        assertEquals(ImmutableSet.copyOf(lastIds), ImmutableSet.of(elemRemote.id()));
    }

    @Test
    void getOrLoadLabelIn() {
        final ImmutableSet<String> labels = ImmutableSet.of("a0", "c2");
        // when: 'loading the synchronous and remote element'
        final Stream<Neo4JElement> actual = sut.getOrLoadLabelIn(graph, labels);
        // then: 'the synchronous element and the remote element are returned'
        assertEquals(ImmutableSet.of(elemSync, elemRemote), actual.collect(Collectors.toSet()));
        // and: 'the last ids loaded only contain the remote element and the graph was used'
        assertEquals(graph, lastGraph);
        assertEquals(ImmutableSet.copyOf(lastIds), ImmutableSet.of(elemRemote.id()));
        assertEquals(labels, lastLabels);
    }

    @Test
    void flush() {
        // when: 'adding a transient element'
        sut.add(elemTransient);
        // and: 'flushing the scope'
        sut.flush();
        // then: 'no elements are held by the scope'
        assertTrue(sut.elements.<Boolean>get(Map::isEmpty));
        // and: 'no statement is being issued'
        verify(executor, never()).executeStatement(any());
    }

    @Test
    void getIdAdapter() {
        assertNotNull(sut.getIdAdapter());
    }

    @Test
    void getPropertyFactory() {
        assertNotNull(sut.getPropertyFactory());
    }

    @Test
    void getReadPartition() {
        assertNotNull(sut.getReadPartition());
    }

    private Neo4JElement element(final Neo4JElementId<?> id,
                                 final ImmutableSet<String> labels,
                                 final ImmutableMap<String, Object> props) {

        final Optional<MapAccessor> propAccess = id.isRemote() ? Optional.of(mockMapAccessor(props)) : Optional.empty();

        final Neo4JElement element = new Neo4JElement(graph, id, labels, propAccess, propertyFactory) {

            @Override
            public <V> Property<V> property(final String key, final V value) {
                return property(key, value, Property::empty);
            }

            @Override
            public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
                return properties(Property::empty, propertyKeys);
            }
        };
        return element;
    }

    private static StatementResult mockRecordWithId(final long id) {
        final StatementResult result = mock(StatementResult.class);
        final Record record = mock(Record.class);
        when(result.hasNext()).thenReturn(true);
        when(result.next()).thenReturn(record);
        final Value value = mock(Value.class);
        when(value.asObject()).thenReturn(id);
        when(record.get(0)).thenReturn(value);
        return result;
    }

}