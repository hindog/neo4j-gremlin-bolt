package ta.nemahuta.neo4j.state;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * A property value which wraps a property value and holds an id and the cardinality.
 *
 * @param <T> the cardinality of the value
 * @author Christian Heike (christian.heike@icloud.com)
 */
@EqualsAndHashCode(exclude = "id")
@ToString
public class PropertyValue<T> {

    private final Iterable<T> wrapped;

    @Getter(onMethod = @__(@Nonnull))
    private final PropertyCardinality cardinality;
    @Getter(onMethod = @__(@Nonnull))
    private final Object id;

    /**
     * Construct a new set based property value.
     *
     * @param id      the id of the property value
     * @param wrapped the wrapped value
     */
    public PropertyValue(@Nonnull @NonNull final Object id,
                         @Nonnull @NonNull final Set<T> wrapped) {
        this(id, wrapped, PropertyCardinality.SET);
    }

    /**
     * Constructs a new list based property value.
     *
     * @param id      the id of the value
     * @param wrapped the wrapped value
     */
    public PropertyValue(@Nonnull @NonNull final Object id,
                         @Nonnull @NonNull final List<T> wrapped) {
        this(id, wrapped, PropertyCardinality.LIST);
    }

    /**
     * Constructs a new single value based property value.
     *
     * @param id      the id of the value
     * @param wrapped the wrapped property
     */
    public PropertyValue(@Nonnull @NonNull final Object id,
                         @Nonnull @NonNull final Optional<T> wrapped) {
        this(id, wrapped.map(Collections::singleton).orElse(Collections.emptySet()), PropertyCardinality.SINGLE);
    }

    /**
     * Construct a new property value based on the provided parameters.
     *
     * @param id          the id of the value
     * @param wrapped     the wrapped value as iterable
     * @param cardinality the cardinality
     */
    protected PropertyValue(final Object id,
                            final Iterable<T> wrapped,
                            final PropertyCardinality cardinality) {
        this.id = id;
        this.wrapped = Objects.requireNonNull(wrapped);
        this.cardinality = Objects.requireNonNull(cardinality);
    }

    /**
     * @return the wrapped value as a list
     */
    public ImmutableList<T> asList() {
        return ImmutableList.copyOf(wrapped);
    }

    /**
     * @return the wrapped value as a set
     */
    public ImmutableSet<T> asSet() {
        return ImmutableSet.copyOf(wrapped);
    }

    /**
     * @return the wrapped value as an optional
     */
    public Optional<T> asOptional() {
        final Iterator<T> iterator = wrapped.iterator();
        return iterator.hasNext() ? Optional.ofNullable(iterator.next()) : Optional.empty();
    }

    /**
     * @return the wrapped value as an object depending on its cardinality
     */
    public Object asObject() {
        switch (cardinality) {
            case SET:
                return asSet();
            case LIST:
                return asList();
            case SINGLE:
                return asOptional().orElse(null);
            default:
                throw new IllegalArgumentException("Could not determine how to handle property cardinality: " + cardinality);
        }
    }

    /**
     * Change the cardinality and value for the {@link PropertyValue} and create a new one.
     *
     * @param cardinality the cardinality to be used
     * @param value       the value to be used
     * @return the new {@link PropertyValue}
     */
    @Nonnull
    public PropertyValue<T> with(@Nonnull @NonNull final PropertyCardinality cardinality,
                                 @Nonnull @NonNull final T value) {
        if (!Objects.equals(cardinality, this.cardinality)) {
            throw new IllegalArgumentException("Cannot change from cardinality " + this.cardinality + " to " + cardinality + ".");
        }
        return from(this.id, value, cardinality);
    }

    /**
     * Construct a new {@link PropertyValue} from the provided parameters.
     * Converts the value into an {@link Iterable}, if necessary.
     *
     * @param id          the identifier of the property
     * @param value       the value to be used
     * @param cardinality the cardinality
     * @param <T>         the type of the value
     * @return the new {@link PropertyValue}
     */
    public static <T> PropertyValue<T> from(@Nonnull @NonNull final Object id,
                                            @Nonnull @NonNull final T value,
                                            @Nonnull @NonNull final PropertyCardinality cardinality) {
        final Iterable<T> wrapped = value instanceof Iterable ? (Iterable<T>) value : Collections.singleton(value);
        return new PropertyValue<>(id, wrapped, cardinality);
    }
}

