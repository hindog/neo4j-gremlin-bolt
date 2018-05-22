package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import ta.nemahuta.neo4j.scope.Neo4JElementStateScope;
import ta.nemahuta.neo4j.state.Neo4JElementState;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Neo4JElementTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JElementStateScope<Neo4JElementState> scope;

    @Mock
    private Neo4JElementState state, changedState;

    @Mock
    private Property property;

    private Neo4JElement<Neo4JElementState, Property> sut;

    @BeforeEach
    void createSut() {
        sut = new Neo4JElement<Neo4JElementState, Property>(graph, 1l, scope) {
            @Override
            protected Property createNewProperty(final String key) {
                return property;
            }

            @Override
            public String label() {
                return "x";
            }

            @Override
            public <V> Property<V> property(final String key, final V value) {
                return (Property<V>) getProperty(key, value);
            }

            @Override
            public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
                return getProperties(propertyKeys).map(p -> (Property<V>) p).iterator();
            }
        };
        when(scope.get(1l)).thenReturn(state);
        when(state.withProperties(any())).thenReturn(changedState);
        when(state.getProperties()).thenReturn(ImmutableMap.of("x", "y"));
        when(changedState.getProperties()).thenReturn(ImmutableMap.of("x", "y", "a", "b"));
    }

    @Test
    void id() {
        assertEquals(Long.valueOf(1l), sut.id());
    }

    @Test
    void graph() {
        assertEquals(graph, sut.graph());
    }

    @Test
    void property() {
        // expect: 'known property returns a created one'
        assertEquals(property, sut.property("x"));
    }

    @Test
    void changePropertyValueSameValue() {
        // when: 'setting the same value'
        sut.property("x", "y");
        // then: 'there is no update to the scope'
        verify(scope, never()).update(eq(1l), any());
    }

    @Test
    void changePropertyValueDifferentOne() {
        // when: 'updating the property'
        sut.property("x", "z");
        // then: 'the value has been updated'
        verify(scope, times(1)).update(eq(1l), any());
    }

    @Test
    void updatePropertyValue() {
        // when: 'adding a property'
        assertEquals(property, sut.property("a", "b"));
        // then: 'there was an update to the properties'
        verify(scope, times(1)).update(eq(1l), any());
    }

    @Test
    void getProperties() {
        // setup: 'the properties in the state'
        when(state.getProperties()).thenReturn(ImmutableMap.of("x", "y"));
        // expect: 'the iterator contains the correct properties'
        assertEquals(ImmutableList.of(property, property), ImmutableList.copyOf(sut.properties("x", "y")));
    }

    @Test
    void remove() {
        // when: 'removing the item'
        sut.remove();
        // then: 'it is removed from the scope'
        verify(scope, times(1)).delete(1l);
    }

    @Test
    void checkHashCode() {
        // expect: 'the hashcode to be the same as the id'
        assertEquals(Long.valueOf(1l).hashCode(), sut.hashCode());
    }
}