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

import javax.annotation.Nonnull;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.types.TypeConstructor.*;

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
        when(result[0].get(anyString())).thenAnswer(i -> mockValue(props.get(i.getArgument(0))));
        return result[0];
    }

    @Nonnull
    public static Value mockValue(final Object prop) {
        final Value value = mock(Value.class);
        if (prop == null) {
            when(value.type()).thenReturn(new TypeRepresentation(NULL_TyCon));
        } else if (prop instanceof Boolean) {
            when(value.asBoolean()).thenReturn((Boolean) prop);
            when(value.type()).thenReturn(new TypeRepresentation(BOOLEAN_TyCon));
        } else if (prop instanceof String) {
            when(value.asString()).thenReturn((String) prop);
            when(value.type()).thenReturn(new TypeRepresentation(STRING_TyCon));
        } else if (prop instanceof Float) {
            when(value.asFloat()).thenReturn((Float) prop);
            when(value.type()).thenReturn(new TypeRepresentation(FLOAT_TyCon));
        } else if (prop instanceof Integer) {
            when(value.asInt()).thenReturn((Integer) prop);
            when(value.type()).thenReturn(new TypeRepresentation(INTEGER_TyCon));
        } else if (prop instanceof Number) {
            when(value.asNumber()).thenReturn((Number) prop);
            when(value.type()).thenReturn(new TypeRepresentation(NUMBER_TyCon));
        } else if (prop instanceof byte[]) {
            when(value.asByteArray()).thenReturn((byte[]) prop);
            when(value.type()).thenReturn(new TypeRepresentation(BYTES_TyCon));
        } else if (prop instanceof Iterable) {
            when(value.asList()).thenReturn(ImmutableList.copyOf((Iterable) prop));
            when(value.type()).thenReturn(new TypeRepresentation(LIST_TyCon));
        } else {
            when(value.asObject()).thenReturn(prop);
            when(value.type()).thenReturn(new TypeRepresentation(ANY_TyCon));
        }
        return value;
    }

}
