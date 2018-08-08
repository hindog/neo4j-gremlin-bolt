package ta.nemahuta.neo4j.cache;

public class NopSessionCacheManager extends AbstractSessionCacheManager {

    @Override
    public SessionCache createSessionCache(final Object id) {
        return new DefaultSessionCache(
                new NopHierarchicalCache<>(),
                createIdCache(globalKnownVertexIds),
                new NopHierarchicalCache<>(),
                createIdCache(globalKnownEdgeIds)
        );
    }

    @Override
    public void close() {
    }
}
