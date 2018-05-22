package ta.nemahuta.neo4j.session;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import ta.nemahuta.neo4j.structure.EdgeProvider;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Lazy implementation of the {@link EdgeProvider} which caches the loaded edges, even if edges with some labels have been loaded.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class LazyEdgeProvider implements EdgeProvider {

    private final ConcurrentMap<String, Set<Long>> cache = new ConcurrentHashMap<>();
    private final Set<String> loadedLabels = new HashSet<>();

    private final Function<Set<String>, Map<String, Set<Long>>> retriever;

    private boolean completelyLoaded;

    public LazyEdgeProvider(@Nonnull @NonNull final Function<Set<String>, Map<String, Set<Long>>> retriever,
                            final boolean completelyLoaded) {
        this.completelyLoaded = completelyLoaded;
        this.retriever = retriever;
    }

    @Override
    @Nonnull
    public Collection<Long> provideEdges(@Nonnull @NonNull final String... labels) {
        // Load the edges if necessary
        final ImmutableSet<String> labelSet = ImmutableSet.copyOf(labels);
        computeSelectorAndLoad(labelSet);
        final ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        fromCache(labelSet).forEach(builder::addAll);
        return builder.build();
    }

    @Override
    public void register(@Nonnull final String label, final long id) {
        putToCache(label, Collections.singleton(id));
    }


    /**
     * Computes the selector and loads the edges for the provided labels, putting them to the cache.
     *
     * @param labelSet the labels to be loaded
     */
    protected void computeSelectorAndLoad(@Nonnull @NonNull final ImmutableSet<String> labelSet) {
        computeSelector(labelSet).ifPresent(selector -> {
            retriever.apply(selector.includeLabels)
                    .entrySet()
                    .stream()
                    .filter(e -> !selector.excludeLabels.contains(e.getKey()))
                    .forEach(e -> {
                        this.loadedLabels.add(e.getKey());
                        this.putToCache(e.getKey(), e.getValue());
                    });
            if (labelSet.isEmpty()) {
                // If we had to load every edge, just mark this provider to be completely loaded
                completelyLoaded = true;
            }
        });
    }


    /**
     * Streams the edges for the provided labels from the cache.
     *
     * @param labels the labels to query the edges for
     * @return the stream providing a set of labels from the cache
     */
    protected Stream<Set<Long>> fromCache(@Nonnull @NonNull final ImmutableSet<String> labels) {
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
     * @param label the label for the id
     * @param ids   the ids to put to the cache
     */
    private void putToCache(final String label, final Collection<Long> ids) {
        Optional.ofNullable(cache.get(label))
                .orElseGet(() -> {
                    final Set<Long> edges = new HashSet<>();
                    cache.put(label, edges);
                    return edges;
                }).addAll(ids);
    }


    @RequiredArgsConstructor
    public static class EdgeLoadSelector {

        private final ImmutableSet<String> includeLabels;
        private final ImmutableSet<String> excludeLabels;

    }

}
