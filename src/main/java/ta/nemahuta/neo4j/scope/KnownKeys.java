package ta.nemahuta.neo4j.scope;

import lombok.Getter;
import ta.nemahuta.neo4j.session.RollbackAndCommit;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KnownKeys<K> implements RollbackAndCommit {

    private final Optional<KnownKeys<K>> parent;

    @Getter
    private boolean completelyKnown;

    @Getter(onMethod = @__(@Nonnull))
    private final Set<K> loaded;

    @Getter(onMethod = @__(@Nonnull))
    private final Set<K> removed;

    public KnownKeys() {
        this.completelyKnown = false;
        this.loaded = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.removed = Collections.emptySet();
        this.parent = Optional.empty();
    }

    public KnownKeys(@Nonnull final KnownKeys<K> parent) {
        this.completelyKnown = parent.isCompletelyKnown();
        this.loaded = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.removed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.parent = Optional.of(parent);
    }

    @Nonnull
    public Set<K> getExisting() {
        final HashSet<K> result = new HashSet<>(loaded);
        parent.ifPresent(p -> result.addAll(p.loaded));
        result.removeAll(removed);
        return result;
    }

    /**
     * Mark the existing to be completely existing
     */
    public void markCompletelyKnown() {
        this.completelyKnown = true;
    }

    /**
     * Commit the existing existing.
     */
    @Override
    public void commit() {
        parent.ifPresent(p -> {
            if (isCompletelyKnown()) {
                p.completelyKnown = this.completelyKnown;
            }
            p.loaded.addAll(this.loaded);
            p.loaded.removeAll(this.removed);
        });
        this.removed.clear();
        this.loaded.clear();
    }

    @Override
    public void rollback() {
        this.removed.clear();
        this.loaded.clear();
        this.completelyKnown = parent.map(KnownKeys::isCompletelyKnown).orElse(false);
    }

}
