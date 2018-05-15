package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.PropertyValue;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static ta.nemahuta.neo4j.query.AbstractStatementBuilderTest.prop;

@ExtendWith(MockitoExtension.class)
class AbstractNeo4JElementScopeTest {

    private final Neo4JGraph graph = mock(Neo4JGraph.class);
    private final Neo4JElement elemSync = element(SyncState.SYNCHRONOUS, new Neo4JPersistentElementId<>(1l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b")),
            elemTransient = element(SyncState.TRANSIENT, new Neo4JTransientElementId<>(1l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b")),
            elemModified = element(SyncState.MODIFIED, new Neo4JPersistentElementId<>(2l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b")),
            elemDeleted = element(SyncState.DELETED, new Neo4JPersistentElementId<>(3l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b")),
            elemDiscarded = element(SyncState.DISCARDED, new Neo4JPersistentElementId<>(2l), ImmutableSet.of("x", "y"), ImmutableMap.of("a", "b"));

    @Mock
    private Statement deleteStmt, updateStmt, insertStmt;
    @Mock
    private StatementExecutor executor;

    private AbstractNeo4JElementScope<Neo4JElement> sut;

    @BeforeEach
    void createSutAndStub() {
        this.sut = new AbstractNeo4JElementScope<Neo4JElement>(ImmutableMap.of(elemSync.id(), elemSync), new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.anyLabel(), executor) {
            @Override
            protected Stream<? extends Neo4JElementId<?>> idsWithLabelIn(final Set<String> labels) {
                return Stream.empty();
            }

            @Override
            protected Stream<Neo4JElement> load(@Nonnull final Neo4JGraph graph, @Nonnull final Iterable<? extends Neo4JElementId<?>> ids) {
                return Stream.empty();
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
            assertFalse(c.containsKey(previousId));
            assertTrue(c.containsKey(elemTransient.id()));
        });
    }

    @Test
    void commitModified() {
        // setup: 'a deleted element'
        sut.add(elemModified);
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, times(1)).executeStatement(updateStmt);
        assertEquals(SyncState.SYNCHRONOUS, elemModified.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertTrue(c.containsKey(elemModified.id())));
    }

    @Test
    void commitDeleted() {
        // setup: 'a deleted element'
        sut.add(elemDeleted);
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, times(1)).executeStatement(deleteStmt);
        assertEquals(SyncState.DISCARDED, elemDeleted.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertFalse(c.containsKey(elemDeleted.id())));
    }

    @Test
    void commitDiscarded() {
        // setup: 'a discarded element'
        sut.add(elemDiscarded);
        // when: 'committing the transaction'
        sut.commit();
        // then: 'the delete statement was executed'
        verify(executor, never()).executeStatement(any());
        assertEquals(SyncState.DISCARDED, elemDiscarded.getState().getCurrentSyncState());
        sut.elements.consume(c -> assertFalse(c.containsKey(elemDiscarded.id())));
    }

    @Test
    void rollback() {
    }

    @Test
    void getOrLoad() {
    }

    @Test
    void getOrLoadLabelIn() {
    }

    @Test
    void flush() {
    }

    @Test
    void getIdAdapter() {
    }

    @Test
    void getPropertyIdGenerator() {
    }

    @Test
    void getReadPartition() {
    }

    private Neo4JElement element(final SyncState syncState,
                                 final Neo4JElementId<?> id,
                                 final ImmutableSet<String> labels,
                                 final ImmutableMap<String, Object> props) {
        final ImmutableMap<String, PropertyValue<?>> properties = ImmutableMap.copyOf(Maps.transformValues(props, v -> prop(v)));
        return new Neo4JElement(graph, new StateHolder<>(syncState, new Neo4JElementState(id, labels, properties))) {
            @Override
            public <V> Property<V> property(final String key, final V value) {
                return null;
            }

            @Override
            public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
                return null;
            }
        };
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