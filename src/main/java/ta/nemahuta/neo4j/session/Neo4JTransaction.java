package ta.nemahuta.neo4j.session;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import ta.nemahuta.neo4j.structure.Neo4JGraph;

import javax.annotation.Nonnull;

/**
 * Gremlin based wrapper for wrapping the transactions of a {@link Neo4JSession}.
 *
 * @author Christian Heike (christian.heike@icloud.com)
 */
@Slf4j
public class Neo4JTransaction extends AbstractThreadedTransaction {

    private final Neo4JSession session;

    public Neo4JTransaction(@Nonnull @NonNull final Neo4JGraph g,
                            @Nonnull @NonNull final Neo4JSession session) {
        super(g);
        this.session = session;
    }

    @Override
    protected void doOpen() {
        session.txOpen();
    }

    @Override
    protected void doCommit() throws TransactionException {
        log.debug("Committing all entities in sessions scope of session: {}", session.hashCode());
        session.getScope().commit();
        session.txCommit();
    }

    @Override
    protected void doRollback() throws TransactionException {
        log.debug("Rolling back all entities in sessions scope of session: {}", session.hashCode());
        session.getScope().commit();
        session.txRollback();
    }

    @Override
    public boolean isOpen() {
        return session.isTxOpen();
    }

}
