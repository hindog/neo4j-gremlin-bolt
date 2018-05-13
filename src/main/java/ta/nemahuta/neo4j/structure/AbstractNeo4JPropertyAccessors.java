package ta.nemahuta.neo4j.structure;

import lombok.NonNull;

import javax.annotation.Nonnull;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An abstract class providing a {@link SoftReference} based cache to the {@link Neo4JProperty} accessory being used for gremlin.
 * @param <P> the parent {@link Neo4JElement} type
 * @param <T> the type of the {@link Neo4JProperty}
 */
public abstract class AbstractNeo4JPropertyAccessors<P extends Neo4JElement, T extends Neo4JProperty<P, ?>> {

    /**
     * soft-reference based cache storing already constructed accessors
     */
    private final ConcurrentMap<String, Reference<T>> cache = new ConcurrentHashMap<>();

    /**
     * Create a {@link Neo4JProperty} accessor for a certain key.
     * @param name the key to be used
     * @return the constructed accessor
     */
    @Nonnull
    protected abstract T createProperty(@Nonnull final String name);

    /**
     * Get a {@link Neo4JProperty} for a certain key, either from cache or constructing a new one.
     * @param name the key to be used
     * @return the property accessor
     */
    public T get(@Nonnull @NonNull final String name) {
        return Optional.ofNullable(cache.get(name))
                .map(Reference::get)
                .orElseGet(() -> {
                    final T result = createProperty(name);
                    cache.put(name, new SoftReference<>(result));
                    return result;
                });
    }

}
