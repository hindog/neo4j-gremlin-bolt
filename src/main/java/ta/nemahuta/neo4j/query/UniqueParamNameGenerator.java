package ta.nemahuta.neo4j.query;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A generator for unique parameter names.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class UniqueParamNameGenerator {

    private final ConcurrentMap<String, AtomicLong> prefixesCounter = new ConcurrentHashMap<>();

    /**
     * Generate a new unique name.
     *
     * @param prefix the prefix for the name
     * @return the prefix plus an appended number (collision free)
     */
    @Nonnull
    public String generate(@Nonnull final String prefix) {
        return prefix + (prefixesCounter.computeIfAbsent(prefix, k -> new AtomicLong(0)).incrementAndGet());
    }

}
