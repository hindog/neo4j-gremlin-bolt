package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.state.Neo4JVertexState;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Neo4JVertexPropertyTest {
    @Mock
    private Neo4JVertex parent;

    @Mock
    private Neo4JVertexState state;

    private Neo4JVertexProperty sutXy, sutYz;

    @BeforeEach
    void createProperty() {
        when(parent.id()).thenReturn(1l);
        when(parent.getState()).thenReturn(state);
        when(state.getProperties()).thenReturn(ImmutableMap.of("yz", "z"));
        sutXy = new Neo4JVertexProperty(parent, "xy");
        sutYz = new Neo4JVertexProperty(parent, "yz");
    }


    @Test
    void emptyProperty() {
        assertFalse(sutXy.isPresent());
        assertThrows(IllegalStateException.class, () -> sutXy.id());
        assertThrows(IllegalStateException.class, () -> sutXy.properties());
        assertThrows(IllegalStateException.class, () -> sutXy.property("x"));
        assertThrows(IllegalStateException.class, () -> sutXy.property("x", "y"));
    }

    @Test
    void existingProperty() {
        assertTrue(sutYz.isPresent());
        assertEquals("1.yz", sutYz.id());
        assertThrows(IllegalStateException.class, () -> sutXy.properties());
        assertThrows(IllegalStateException.class, () -> sutXy.property("x"));
        assertThrows(IllegalStateException.class, () -> sutXy.property("x", "y"));
        assertNotNull(sutYz.toString());
    }

}