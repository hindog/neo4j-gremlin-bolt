package ta.nemahuta.neo4j.id;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class Neo4JTransientElementIdTest {

    @Test
    void nullValueNotAllowed() {
        assertThrows(RuntimeException.class, () -> new Neo4JTransientElementId<>(null));
    }

}