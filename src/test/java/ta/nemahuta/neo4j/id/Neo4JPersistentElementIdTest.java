package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class Neo4JPersistentElementIdTest {

    @Test
    void equalsToStringAndRemote() {
        // setup: 'some element ids'
        final Neo4JElementId<?> id1 = new Neo4JPersistentElementId<>("1");
        final Neo4JElementId<?> id2 = new Neo4JPersistentElementId<>(1l);
        final Neo4JElementId<?> id3 = new Neo4JPersistentElementId<>(1l);
        final Neo4JElementId<?> id4 = new Neo4JTransientElementId<>(1l);
        final Neo4JElementId<?> id5 = new Neo4JTransientElementId<>("1");
        // expect: 'the hashcode and equals is stable'
        assertNotEquals(id1.hashCode(), id2.hashCode());
        assertNotEquals(id1.hashCode(), id3.hashCode());
        assertEquals(id2.hashCode(), id2.hashCode());
        assertNotEquals(id4.hashCode(), id5.hashCode());
        assertFalse(id1.equals(id2));
        assertFalse(id1.equals(id3));
        assertTrue(id2.equals(id3));
        assertFalse(id3.equals(id4));
        assertFalse(id4.equals(id5));
        // and: 'toString never returns null'
        assertFalse(Stream.of(id1, id2, id3, id4, id5).map(Object::toString).filter(Objects::isNull).findAny().isPresent());
    }

}