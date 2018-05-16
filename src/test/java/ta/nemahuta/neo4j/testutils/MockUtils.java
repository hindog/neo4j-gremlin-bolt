package ta.nemahuta.neo4j.testutils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.property.AbstractPropertyFactory;
import ta.nemahuta.neo4j.structure.Neo4JElement;
import ta.nemahuta.neo4j.structure.Neo4JProperty;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.types.TypeConstructor.LIST_TyCon;
import static org.neo4j.driver.internal.types.TypeConstructor.STRING_TyCon;

public class MockUtils {

    public static ImmutableMap<String, ? extends Neo4JProperty<? extends Neo4JElement, ?>> mockProperties(final Map<String, Object> props) {
        return ImmutableMap.copyOf(Maps.transformEntries(props, (k, v) -> {
            final Neo4JProperty result = mock(Neo4JProperty.class);
            final Pair<VertexProperty.Cardinality, Iterable<?>> params = AbstractPropertyFactory.getCardinalityAndIterable(v);
            when(result.key()).thenReturn(k);
            when(result.getCardinality()).thenReturn(params.getValue0());
            when(result.value()).thenReturn(v);
            return result;
        }));
    }

    public static MapAccessor mockMapAccessor(final Map<String, Object> props) {
        final MapAccessor[] result = {mock(MapAccessor.class)};
        when(result[0].keys()).thenReturn(props.keySet());
        when(result[0].get(anyString())).thenAnswer(i -> {
            final Object prop = props.get(i.getArgument(0));
            final Value value = mock(Value.class);
            if (prop instanceof Iterable) {
                when(value.asList()).thenReturn(ImmutableList.copyOf((Iterable) prop));
                when(value.type()).thenReturn(new TypeRepresentation(LIST_TyCon));
            } else if (prop instanceof String) {
                when(value.asString()).thenReturn((String) prop);
                when(value.type()).thenReturn(new TypeRepresentation(STRING_TyCon));
            } else {
                throw new IllegalArgumentException("Cannot handle property " + i.getArgument(0) + ": " + prop);
            }
            return value;
        });
        return result[0];
    }
}
