package ta.nemahuta.neo4j.cache;

public interface SessionCacheManager extends AutoCloseable {

    SessionCache createSessionCache(final Object id);

}
