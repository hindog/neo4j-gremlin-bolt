package ta.nemahuta.neo4j.session.cache;

public interface SessionCacheManager extends AutoCloseable {

    SessionCache createSessionCache(final Object id);

}
