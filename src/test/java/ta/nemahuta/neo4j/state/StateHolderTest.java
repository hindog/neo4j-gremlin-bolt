package ta.nemahuta.neo4j.state;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StateHolderTest {

    @Test
    void modify() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.SYNCHRONOUS, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.modify("y");
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.MODIFIED, actual.getSyncState());
        assertEquals("y", actual.getState());
    }

    @Test
    void modifyNoChange() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.SYNCHRONOUS, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.modify("x");
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.SYNCHRONOUS, actual.getSyncState());
        assertEquals("x", actual.getState());
    }

    @Test
    void deleteSynchronous() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.SYNCHRONOUS, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.delete();
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.DELETED, actual.getSyncState());
    }

    @Test
    void deleteTransient() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.TRANSIENT, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.delete();
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.DISCARDED, actual.getSyncState());
    }

    @Test
    void syncedDeleted() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.DELETED, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.synced("y");
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.DISCARDED, actual.getSyncState());
        assertEquals("y", actual.getState());
    }

    @Test
    void syncedTransient() {
        final StateHolder<String> sut = new StateHolder<>(SyncState.TRANSIENT, "x");
        // when: 'updating the state holder'
        final StateHolder<String> actual = sut.synced("y");
        // then: 'the state holder is in a modified state'
        assertEquals(SyncState.SYNCHRONOUS, actual.getSyncState());
        assertEquals("y", actual.getState());
    }

    @Test
    void equals() {
        assertFalse(new StateHolder<>(SyncState.TRANSIENT, "x").equals(new StateHolder<>(SyncState.DISCARDED, "x")));
        assertFalse(new StateHolder<>(SyncState.TRANSIENT, "x").equals(new StateHolder<>(SyncState.TRANSIENT, "y")));
        assertTrue(new StateHolder<>(SyncState.TRANSIENT, "x").equals(new StateHolder<>(SyncState.TRANSIENT, "x")));
    }

}