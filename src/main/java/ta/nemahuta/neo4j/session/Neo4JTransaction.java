package ta.nemahuta.neo4j.session;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import ta.nemahuta.neo4j.session.cache.SessionCache;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Gremlin based wrapper for wrapping the transactions of a {@link Neo4JSession}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public class Neo4JTransaction extends AbstractThreadedTransaction implements StatementExecutor {

    private final Session session;
    private final SessionCache sessionCache;
    private Transaction wrapped;

    public Neo4JTransaction(@Nonnull @NonNull final Neo4JGraph g,
                            @Nonnull @NonNull final Session session,
                            @Nonnull @NonNull final SessionCache sessionCache) {
        super(g);
        this.session = session;
        this.sessionCache = sessionCache;
    }

    @Override
    protected synchronized void doOpen() {
        this.wrapped = Optional.ofNullable(this.wrapped).orElseGet(session::beginTransaction);
    }

    @Override
    protected void doCommit() throws TransactionException {
        log.debug("Committing all entities in sessions scope of session: {}", session.hashCode());
        sessionCache.commit();
        wrapped.success();
    }

    @Override
    protected void doRollback() throws TransactionException {
        log.debug("Rolling back all entities in sessions scope of session: {}", session.hashCode());
        sessionCache.flush();
        wrapped.failure();
    }

    @Override
    public void close() {
        super.close();
        sessionCache.close();
    }

    @Override
    public boolean isOpen() {
        return wrapped != null;
    }

    @Nullable
    @Override
    public StatementResult executeStatement(@Nonnull final Statement statement) {
        readWrite();
        return wrapped.run(statement);
    }
}
