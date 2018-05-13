package ta.nemahuta.neo4j.structure;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.MapAccessor;
import ta.nemahuta.neo4j.id.Neo4JElementIdGenerator;
import ta.nemahuta.neo4j.session.Neo4JElementScope;
import ta.nemahuta.neo4j.state.PropertyValue;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PropertyValueFactory {

    private final Set<String> excludedKeys;
    private final Neo4JElementIdGenerator<?> idGenerator;

    public PropertyValueFactory(final Set<String> excludedKeys, final Neo4JElementIdGenerator<?> idGenerator) {
        this.excludedKeys = Objects.requireNonNull(excludedKeys, "excluded keys may not be null");
        this.idGenerator = Objects.requireNonNull(idGenerator, "id generator may not be null");
    }

    public ImmutableMap<String, PropertyValue<?>> create(final MapAccessor node) {
        final ImmutableMap.Builder<String, PropertyValue<?>> builder = ImmutableMap.builder();
        for (final String key : node.keys()) {
            if (!excludedKeys.contains(key)) {
                builder.put(key, create(node.get(key)));
            }
        }
        return builder.build();
    }

    public PropertyValue<?> create(final Value value) {
        final TypeRepresentation type = (TypeRepresentation) value.type();
        final TypeConstructor typeConstructor = type.constructor();

        // process value type
        switch (Optional.ofNullable(typeConstructor)
                .orElseThrow(() -> new IllegalArgumentException("Encountered a null typed property"))) {
            case LIST_TyCon:
                return new PropertyValue<>(idGenerator.generate(), value.asList());
            case BOOLEAN_TyCon:
            case BYTES_TyCon:
            case FLOAT_TyCon:
            case INTEGER_TyCon:
            case NULL_TyCon:
            case NUMBER_TyCon:
            case STRING_TyCon:
                return new PropertyValue<>(idGenerator.generate(), Optional.ofNullable(value.asObject()));
            default:
                throw new IllegalArgumentException("Determined unhandled type: " + typeConstructor.typeName());
        }
    }

    public static PropertyValueFactory forScope(final Neo4JElementScope<? extends Neo4JElement> scope) {
        return new PropertyValueFactory(Collections.singleton(scope.getIdAdapter().propertyName()), scope.getPropertyIdGenerator());
    }

}
