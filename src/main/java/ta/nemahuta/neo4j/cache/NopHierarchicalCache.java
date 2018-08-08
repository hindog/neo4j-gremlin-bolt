package ta.nemahuta.neo4j.cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Stream;

public class NopHierarchicalCache<K, V> implements HierarchicalCache<K, V> {

    @Nullable
    @Override
    public Object get(@Nonnull final Object key) {
        return null;
    }

    @Override
    public void put(@Nonnull final Object key, @Nonnull final Object value) {
    }

    @Override
    public boolean remove(@Nonnull final Object key) {
        return false;
    }

    @Override
    public void removeFromParent(@Nonnull final Set keys) {
    }

    @Override
    public void commit() {
    }

    @Override
    public void clear() {
    }

    @Override
    public Stream getKeys() {
        return Stream.empty();
    }
}
