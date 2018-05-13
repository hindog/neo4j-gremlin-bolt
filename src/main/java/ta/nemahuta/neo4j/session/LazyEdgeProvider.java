package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableSet;
import lombok.*;
import ta.nemahuta.neo4j.structure.EdgeProvider;
import ta.nemahuta.neo4j.structure.Neo4JEdge;
import ta.nemahuta.neo4j.structure.Neo4JVertex;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Lazy implementation of the {@link EdgeProvider} which caches the loaded edges, even if edges with some labels have been loaded.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class LazyEdgeProvider implements EdgeProvider {

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<String, Set<Neo4JEdge>> cache = new HashMap<>();
    private final Set<String> loadedLabels = new HashSet<>();

    private final Neo4JVertex vertex;
    private final Function<Iterable<String>, Stream<Neo4JEdge>> retriever;

    private boolean completelyLoaded;

    public LazyEdgeProvider(@Nonnull @NonNull final Neo4JVertex vertex,
                            @Nonnull @NonNull final Function<Iterable<String>, Stream<Neo4JEdge>> retriever,
                            final boolean completelyLoaded) {
        this.vertex = vertex;
        this.completelyLoaded = completelyLoaded;
        this.retriever = retriever;
    }

    @Override
    @Nonnull
    public Iterable<Neo4JEdge> provideEdges(@Nonnull @NonNull final String... labels) {
        final Lock readLock = readWriteLock.readLock();
        try {
            // Load the edges if necessary
            final ImmutableSet<String> labelSet = ImmutableSet.copyOf(labels);
            computeSelectorAndLoad(labelSet);
            readLock.lock();
            final ImmutableSet.Builder<Neo4JEdge> builder = ImmutableSet.builder();
            fromCache(labelSet).forEach(builder::addAll);
            return builder.build();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void registerEdge(@Nonnull @NonNull final Neo4JEdge edge) {
        putToCache(edge);
    }

    /**
     * Computes the selector and loads the edges for the provided labels, putting them to the cache.
     * @param labelSet the labels to be loaded
     */
    protected void computeSelectorAndLoad(@Nonnull @NonNull final ImmutableSet<String> labelSet) {
        // Compute the selector
        computeSelector(labelSet).ifPresent(selector -> {
            // If we have to load edges
            final Lock writeLock = readWriteLock.writeLock();
            try {
                // Make sure we lock this for writing
                writeLock.lock();
                // Now load the edges by the selector, and only put those who have not been excluded to the set
                // plus mark their orLabelsAnd as loaded
                retriever.apply(selector.includeLabels)
                        .filter(e -> !selector.excludeLabels.contains(e.label()))
                        .forEach(e -> {
                            this.putToCache(e);
                            loadedLabels.add(e.label());
                        });
                if (labelSet.isEmpty()) {
                    // If we had to load every edge, just mark this provider to be completely loaded
                    completelyLoaded = true;
                }
            } finally {
                writeLock.unlock();
            }
        });
    }


    /**
     * Streams the edges for the provided labels from the cache.
     * @param labels the labels to query the edges for
     * @return the stream providing a set of labels from the cache
     */
    protected Stream<Set<Neo4JEdge>> fromCache(@Nonnull @NonNull final ImmutableSet<String> labels) {
        if (labels.isEmpty()) {
            return cache.values().stream();
        } else {
            return cache.entrySet().parallelStream().filter(e -> labels.contains(e.getKey())).map(Map.Entry::getValue);
        }
    }

    /**
     * Computes the {@link EdgeLoadSelector} to be used to load the edges for the provided labels, not querying the already loaded ones.
     *
     * @param labels the labels to be loaded
     * @return the optional selector, or {@link Optional#empty()} if none is to be loaded
     */
    @Nonnull
    protected Optional<EdgeLoadSelector> computeSelector(@Nonnull @NonNull final ImmutableSet<String> labels) {
        if (completelyLoaded) {
            // In case the edges have been completely loaded, we select nothing
            return Optional.empty();
        }
        // Compute the orLabelsAnd to be included/excluded when loading them
        final ImmutableSet<String> includeLabels = labels.stream().filter(l -> !loadedLabels.contains(l)).collect(ImmutableSet.toImmutableSet());
        final ImmutableSet<String> excludeLabels = loadedLabels.stream().filter(l -> !labels.contains(l)).collect(ImmutableSet.toImmutableSet());
        if (!labels.isEmpty() && includeLabels.isEmpty()) {
            // In case orLabelsAnd have been selected, but none are to be included, nothing is being selected
            return Optional.empty();
        } else {
            // Otherwise we return a real selector
            return Optional.of(new EdgeLoadSelector(includeLabels, excludeLabels));
        }
    }

    /**
     * Puts an edge to the cache using its label.
     *
     * @param edge the edge to put to the cache
     */
    private void putToCache(@Nonnull @NonNull final Neo4JEdge edge) {
        final Lock writeLock = readWriteLock.writeLock();
        try {
            final String label = edge.label();
            final Set<Neo4JEdge> target = Optional.ofNullable(cache.get(label))
                    .orElseGet(() -> {
                        final Set<Neo4JEdge> edges = new HashSet<>();
                        cache.put(label, edges);
                        return edges;
                    });
            target.add(edge);
        } finally {
            writeLock.unlock();
        }
    }


    @EqualsAndHashCode
    @ToString
    @RequiredArgsConstructor
    public static class EdgeLoadSelector {

        @Getter
        private final ImmutableSet<String> includeLabels;
        @Getter
        private final ImmutableSet<String> excludeLabels;

    }

}
