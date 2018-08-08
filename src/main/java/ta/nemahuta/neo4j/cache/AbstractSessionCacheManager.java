package ta.nemahuta.neo4j.cache;

import ta.nemahuta.neo4j.scope.IdCache;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractSessionCacheManager implements SessionCacheManager {

    protected final AtomicReference<Set<Long>> globalKnownEdgeIds = new AtomicReference<>(new HashSet<>());
    protected final AtomicReference<Set<Long>> globalKnownVertexIds = new AtomicReference<>(new HashSet<>());

    @Nonnull
    protected <K> IdCache<K> createIdCache(@Nonnull final AtomicReference<Set<K>> source) {
        return new IdCache<>(source);
    }

}
