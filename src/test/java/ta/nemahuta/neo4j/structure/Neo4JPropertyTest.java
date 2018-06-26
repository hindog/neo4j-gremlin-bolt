package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class Neo4JPropertyTest {

    @Mock
    private Neo4JElement parent;

    @Mock
    private Neo4JElementState state;

    private Neo4JProperty sutXy, sutYz;

    @BeforeEach
    void createProperty() {
        when(parent.getState()).thenReturn(state);
        when(state.getProperties()).thenReturn(ImmutableMap.of("yz", "z"));
        sutXy = new Neo4JProperty(parent, "xy") {
        };
        sutYz = new Neo4JProperty(parent, "yz") {
        };
    }

    @Test
    void emptyProperty() {
        assertFalse(sutXy.isPresent());
        assertThrows(IllegalStateException.class, () -> sutXy.key());
        assertThrows(IllegalStateException.class, () -> sutXy.value());
        assertThrows(IllegalStateException.class, () -> sutXy.element());
        assertThrows(IllegalStateException.class, () -> sutXy.hashCode());
        assertFalse(sutXy.equals(sutYz));
    }

    @Test
    void existingProperty() {
        assertTrue(sutYz.isPresent());
        assertEquals("yz", sutYz.key);
        assertEquals("z", sutYz.value());
        assertNotNull(sutYz.toString());
        assertFalse(sutYz.equals(sutXy));
        sutYz.remove();
        verify(parent, times(1)).removeProperty("yz");
    }

}