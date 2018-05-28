package ta.nemahuta.neo4j.structure;

import javax.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A key-value pair holder which uses {@link SoftReference}s so the instances can be cleared by the GC.
 *
 * @param <K> the key of the entry
 * @param <V> the value of the entry
 */
public class SoftRefMap<K, V> {

    private final Map<K, SoftReference<V>> wrapped;

    public SoftRefMap() {
        this(new ConcurrentHashMap<>());
    }

    protected SoftRefMap(@Nonnull final Map<K, SoftReference<V>> wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Get or create an entry for the key.
     *
     * @param key     the key to be used
     * @param factory the factory for missing or cleared values
     * @return the entry value
     */
    public V getOrCreate(@Nonnull final K key, @Nonnull final Supplier<V> factory) {
        return Optional.ofNullable(wrapped.get(key))
                .map(SoftReference::get)
                .orElseGet(() -> {
                    final V result = Objects.requireNonNull(factory.get(), "Factored value should not be null.");
                    wrapped.put(key, new SoftReference<>(result));
                    return result;
                });
    }

}
