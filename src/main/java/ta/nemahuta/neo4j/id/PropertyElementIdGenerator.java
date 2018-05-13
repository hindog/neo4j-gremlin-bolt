package ta.nemahuta.neo4j.id;

import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Neo4JElementIdGenerator} for {@link org.apache.tinkerpop.gremlin.structure.Property}s which also are
 * {@link org.apache.tinkerpop.gremlin.structure.Element}s.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
public class PropertyElementIdGenerator implements Neo4JElementIdGenerator<Long> {

    /**
     * The sequence generator
     */
    private final AtomicLong idProvider = new AtomicLong(1l);

    @Override
    public Neo4JElementId<Long> generate() {
        return new Neo4JTransientElementId<>(idProvider.addAndGet(1L));
    }

}
