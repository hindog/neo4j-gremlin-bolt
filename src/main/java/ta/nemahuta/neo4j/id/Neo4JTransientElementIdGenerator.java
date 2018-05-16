package ta.nemahuta.neo4j.id;

import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Neo4JElementIdGenerator} which generates {@link Neo4JTransientElementId}s.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class Neo4JTransientElementIdGenerator implements Neo4JElementIdGenerator<Long> {

    /**
     * The sequence generator
     */
    private final AtomicLong idProvider = new AtomicLong(0l);

    @Override
    public Neo4JElementId<Long> generate() {
        return new Neo4JTransientElementId<>(idProvider.addAndGet(1L));
    }

}
