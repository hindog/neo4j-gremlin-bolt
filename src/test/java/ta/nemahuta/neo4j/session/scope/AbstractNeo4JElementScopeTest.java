package ta.nemahuta.neo4j.session.scope;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.v1.Statement;
import ta.nemahuta.neo4j.id.Neo4JElementId;
import ta.nemahuta.neo4j.id.Neo4JNativeElementIdAdapter;
import ta.nemahuta.neo4j.id.Neo4JPersistentElementId;
import ta.nemahuta.neo4j.id.Neo4JTransientElementId;
import ta.nemahuta.neo4j.partition.Neo4JLabelGraphPartition;
import ta.nemahuta.neo4j.session.StatementExecutor;
import ta.nemahuta.neo4j.state.LocalAndRemoteStateHolder;
import ta.nemahuta.neo4j.state.Neo4JElementState;
import ta.nemahuta.neo4j.state.StateHolder;
import ta.nemahuta.neo4j.state.SyncState;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AbstractNeo4JElementScopeTest {

    @Mock
    private Neo4JElement element1, element2, element3;
    @Mock
    private Statement deleteStmt, updateStmt, insertStmt;
    @Mock
    private StatementExecutor executor;

    private AbstractNeo4JElementScope<Neo4JElement> sut;

    @BeforeEach
    void createSutAndStub() {
        when(element1.id()).thenReturn((Neo4JElementId) new Neo4JPersistentElementId<>(1l));
        this.sut = new AbstractNeo4JElementScope<Neo4JElement>(ImmutableMap.of(element1.id(), element1), new Neo4JNativeElementIdAdapter(), Neo4JLabelGraphPartition.anyLabel(), executor) {
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
        // setup: 'an element to be added'
        when(element2.id()).thenReturn((Neo4JElementId) new Neo4JTransientElementId<>(2l));
        // when: 'adding an element to the scope'
        sut.add(element2);
        // then: 'the element has been put to the map correctly'
        sut.elements.consume(es -> {
            assertTrue(es.containsKey(element2.id()));
            assertTrue(es.containsKey(element1.id()));
        });
    }

    @Test
    void commit() {
        // setup: 'an element to be added'
//        when(element1.getState()).thenReturn(state(element1.id(), SyncState.TRANSIENT));
        //      when(element2.getState()).thenReturn(state(element1.id(), SyncState.TRANSIENT));
        // when: 'committing the transaction'
        //    verify(executor).executeStatement(insertStmt);
    }

    private LocalAndRemoteStateHolder<Neo4JElementState> state(final Neo4JElementId<?> id, final SyncState syncState) {
        return new LocalAndRemoteStateHolder<>(new StateHolder<>(syncState, new Neo4JElementState(id, ImmutableSet.of(), ImmutableMap.of())));
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
}